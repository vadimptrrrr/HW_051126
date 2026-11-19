#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
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

    explicit ThreadPool(unsigned int count);
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ~ThreadPool();

    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    template< typename F, typename... Args >
    auto submit(F&& func, Args&&... args) -> std::future< typename std::result_of< F(Args...) >::type >;

    void waitAll();

  private:
    std::queue< Task > tasks_;
    std::mutex mutex_;
    std::condition_variable taskCond_;
    std::condition_variable doneCond_;
    std::size_t activeTasks_;
    bool stop_;
    std::vector< std::thread > threads_;

    void execute();
  };

  ThreadPool::ThreadPool(unsigned int count):
    activeTasks_(0),
    stop_(false)
  {
    auto worker = [this]()
    {
      execute();
    };

    for (unsigned int i = 0; i < count; ++i)
    {
      threads_.emplace_back(worker);
    }
  }

  ThreadPool::~ThreadPool()
  {
    {
      std::lock_guard< std::mutex > lock(mutex_);
      stop_ = true;
    }
    taskCond_.notify_all();

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

    auto boundTask = std::bind(std::forward< F >(func), std::forward< Args >(args)...);
    auto taskPtr = std::make_shared< std::packaged_task< ReturnType() > >(std::move(boundTask));
    std::future< ReturnType > result = taskPtr->get_future();

    {
      std::lock_guard< std::mutex > lock(mutex_);
      if (stop_)
      {
        throw std::runtime_error("Submit on stopped thread pool");
      }
      tasks_.emplace(
        [taskPtr]()
        {
          (*taskPtr)();
        }
      );
    }

    taskCond_.notify_one();
    return result;
  }

  void ThreadPool::waitAll()
  {
    std::unique_lock< std::mutex > lock(mutex_);
    doneCond_.wait(
      lock,
      [this]()
      {
        return tasks_.empty() && (activeTasks_ == 0);
      }
    );
  }

  void ThreadPool::execute()
  {
    std::unique_lock< std::mutex > lock(mutex_);
    do
    {
      auto proceed = [this]()
      {
        return !tasks_.empty() || stop_;
      };
      taskCond_.wait(lock, proceed);

      if (tasks_.empty())
      {
        continue;
      }

      Task task = std::move(tasks_.front());
      tasks_.pop();
      ++activeTasks_;

      lock.unlock();
      try
      {
        task();
      }
      catch (...)
      {
      }
      lock.lock();

      --activeTasks_;
      if (tasks_.empty() && (activeTasks_ == 0))
      {
        doneCond_.notify_all();
      }
    }
    while (!stop_ || !tasks_.empty());
  }
}

int main()
{
  return 0;
}