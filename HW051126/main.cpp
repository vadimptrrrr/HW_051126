#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace detail
{
  class ThreadPool
  {
  public:
    using Task = std::function< void(void) >;

    static constexpr std::size_t BATCH_SIZE = 8;

    explicit ThreadPool(unsigned int count);
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ~ThreadPool();

    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    template< typename F, typename... Args >
    auto submit(F&& func, Args&&... args) -> std::future< typename std::result_of< F(Args...) >::type >;

    void wait();
    void pause();
    void play();
    void stop();

  private:
    std::queue< Task > globalQueue_;
    std::shared_timed_mutex globalMutex_;
    std::vector< std::deque< Task > > localTasks_;
    std::vector< std::shared_timed_mutex > localMutexes_;
    std::mutex waitMutex_;
    std::condition_variable taskCond_;
    std::condition_variable waitCond_;
    std::atomic< bool > pause_;
    std::atomic< bool > stop_;
    std::atomic< std::size_t > activeTasks_;
    std::vector< std::thread > threads_;

    void execute(std::size_t threadId);
    bool fetchBatch(std::size_t threadId);
    bool trySteal(std::size_t threadId, Task& outTask);
    bool hasPendingWork();
  };

  ThreadPool::ThreadPool(unsigned int count):
    localTasks_(count),
    localMutexes_(count),
    pause_(false),
    stop_(false),
    activeTasks_(0)
  {
    if (count == 0)
    {
      throw std::invalid_argument("Pool size must be > 0");
    }

    threads_.reserve(count);
    for (unsigned int i = 0; i < count; ++i)
    {
      const std::size_t threadId = static_cast< std::size_t >(i);
      threads_.emplace_back(
        [this, threadId]()
        {
          execute(threadId);
        }
      );
    }
  }

  ThreadPool::~ThreadPool()
  {
    stop();
    for (std::size_t i = 0; i < threads_.size(); ++i)
    {
      if (threads_[i].joinable())
      {
        threads_[i].join();
      }
    }
  }

  template< typename F, typename... Args >
  auto ThreadPool::submit(F&& func, Args&&... args) -> std::future< typename std::result_of< F(Args...) >::type >
  {
    using ReturnType = typename std::result_of< F(Args...) >::type;

    if (stop_.load())
    {
      throw std::runtime_error("Submit on stopped pool");
    }

    auto boundTask = std::bind(std::forward< F >(func), std::forward< Args >(args)...);
    auto taskPtr = std::make_shared< std::packaged_task< ReturnType() > >(std::move(boundTask));
    std::future< ReturnType > result = taskPtr->get_future();

    {
      std::unique_lock< std::shared_timed_mutex > writeLock(globalMutex_);
      globalQueue_.push(
        [taskPtr]()
        {
          (*taskPtr)();
        }
      );
    }

    taskCond_.notify_one();
    return result;
  }

  bool ThreadPool::hasPendingWork()
  {
    if (activeTasks_.load() != 0)
    {
      return true;
    }

    {
      std::shared_lock< std::shared_timed_mutex > readLock(globalMutex_);
      if (!globalQueue_.empty())
      {
        return true;
      }
    }

    for (std::size_t i = 0; i < localTasks_.size(); ++i)
    {
      std::shared_lock< std::shared_timed_mutex > readLock(localMutexes_[i]);
      if (!localTasks_[i].empty())
      {
        return true;
      }
    }

    return false;
  }

  void ThreadPool::wait()
  {
    std::unique_lock< std::mutex > lock(waitMutex_);
    waitCond_.wait(
      lock,
      [this]()
      {
        return stop_.load() || !hasPendingWork();
      }
    );
  }

  void ThreadPool::pause()
  {
    pause_.store(true);
  }

  void ThreadPool::play()
  {
    pause_.store(false);
    taskCond_.notify_all();
  }

  void ThreadPool::stop()
  {
    stop_.store(true);
    taskCond_.notify_all();
    waitCond_.notify_all();
  }

  bool ThreadPool::fetchBatch(std::size_t threadId)
  {
    {
      std::shared_lock< std::shared_timed_mutex > readLock(globalMutex_);
      if (globalQueue_.empty())
      {
        return false;
      }
    }

    std::deque< Task > batch;
    {
      std::unique_lock< std::shared_timed_mutex > writeLock(globalMutex_);
      if (globalQueue_.empty())
      {
        return false;
      }

      std::size_t count = 0;
      while ((count < BATCH_SIZE) && !globalQueue_.empty())
      {
        batch.push_back(std::move(globalQueue_.front()));
        globalQueue_.pop();
        ++count;
      }
    }

    {
      std::unique_lock< std::shared_timed_mutex > localWriteLock(localMutexes_[threadId]);
      for (std::size_t i = 0; i < batch.size(); ++i)
      {
        localTasks_[threadId].push_back(std::move(batch[i]));
      }
    }

    if (batch.size() > 1)
    {
      taskCond_.notify_all();
    }

    return true;
  }

  bool ThreadPool::trySteal(std::size_t threadId, Task& outTask)
  {
    const std::size_t queueCount = localTasks_.size();
    for (std::size_t i = 1; i < queueCount; ++i)
    {
      const std::size_t victimId = (threadId + i) % queueCount;
      {
        std::shared_lock< std::shared_timed_mutex > readLock(localMutexes_[victimId]);
        if (localTasks_[victimId].size() < 2)
        {
          continue;
        }
      }

      std::deque< Task > stolenBatch;
      {
        std::unique_lock< std::shared_timed_mutex > victimWriteLock(localMutexes_[victimId]);
        if (localTasks_[victimId].size() < 2)
        {
          continue;
        }

        const std::size_t stealCount = localTasks_[victimId].size() / 2;
        outTask = std::move(localTasks_[victimId].back());
        localTasks_[victimId].pop_back();

        for (std::size_t j = 1; j < stealCount; ++j)
        {
          stolenBatch.push_back(std::move(localTasks_[victimId].back()));
          localTasks_[victimId].pop_back();
        }
      }

      if (!stolenBatch.empty())
      {
        std::unique_lock< std::shared_timed_mutex > thiefWriteLock(localMutexes_[threadId]);
        for (std::size_t j = 0; j < stolenBatch.size(); ++j)
        {
          localTasks_[threadId].push_front(std::move(stolenBatch[j]));
        }
      }

      return true;
    }

    return false;
  }

  void ThreadPool::execute(std::size_t threadId)
  {
    while (!stop_.load())
    {
      if (pause_.load())
      {
        std::unique_lock< std::mutex > lock(waitMutex_);
        taskCond_.wait(
          lock,
          [this]()
          {
            return stop_.load() || !pause_.load();
          }
        );
        continue;
      }

      Task task{};
      bool found = false;

      {
        std::shared_lock< std::shared_timed_mutex > readLock(localMutexes_[threadId]);
        found = !localTasks_[threadId].empty();
      }

      if (found)
      {
        std::unique_lock< std::shared_timed_mutex > writeLock(localMutexes_[threadId]);
        if (!localTasks_[threadId].empty())
        {
          task = std::move(localTasks_[threadId].front());
          localTasks_[threadId].pop_front();
        }
        else
        {
          found = false;
        }
      }

      if (!found)
      {
        if (fetchBatch(threadId))
        {
          std::unique_lock< std::shared_timed_mutex > writeLock(localMutexes_[threadId]);
          if (!localTasks_[threadId].empty())
          {
            task = std::move(localTasks_[threadId].front());
            localTasks_[threadId].pop_front();
            found = true;
          }
        }
      }

      if (!found)
      {
        found = trySteal(threadId, task);
      }

      if (found)
      {
        ++activeTasks_;
        try
        {
          task();
        }
        catch (...)
        {
        }
        --activeTasks_;

        if (!hasPendingWork())
        {
          waitCond_.notify_all();
        }
      }
      else
      {
        std::unique_lock< std::mutex > lock(waitMutex_);
        taskCond_.wait(
          lock,
          [this]()
          {
            return stop_.load() || (!pause_.load() && hasPendingWork());
          }
        );
      }
    }
  }
}

int main()
{
  const unsigned int threadCount = 4;
  const int totalTasks = 32;
  const int heavyTaskStep = 5;
  const int mediumTaskStep = 2;
  const int heavySleepMs = 150;
  const int mediumSleepMs = 40;
  const int fastSleepMs = 5;

  detail::ThreadPool pool(threadCount);
  std::vector< std::future< int > > futures;
  futures.reserve(static_cast< std::size_t >(totalTasks));

  const auto startTime = std::chrono::steady_clock::now();

  for (int i = 0; i < totalTasks; ++i)
  {
    futures.push_back(
      pool.submit(
        [i, heavyTaskStep, mediumTaskStep, heavySleepMs, mediumSleepMs, fastSleepMs]()
        {
          int durationMs = fastSleepMs;
          if ((i % heavyTaskStep) == 0)
          {
            durationMs = heavySleepMs;
          }
          else if ((i % mediumTaskStep) == 0)
          {
            durationMs = mediumSleepMs;
          }

          std::this_thread::sleep_for(std::chrono::milliseconds(durationMs));
          return i + 1;
        }
      )
    );
  }

  pool.wait();

  const auto endTime = std::chrono::steady_clock::now();
  const auto elapsedMs = std::chrono::duration_cast< std::chrono::milliseconds >(endTime - startTime).count();

  int checksum = 0;
  for (std::size_t i = 0; i < futures.size(); ++i)
  {
    checksum += futures[i].get();
  }

  std::cout << "Tasks completed: " << futures.size() << '\n';
  std::cout << "Checksum: " << checksum << '\n';
  std::cout << "Elapsed time (ms): " << elapsedMs << '\n';

  return 0;
}