S：
————————————————————————————————————————————————————————————
key: str
function arguments must have a statically known size, borrowed types always have a known size: `&`rustcE0277
————————————————————————————————————————————————————————————
Q：
str在rust中到底是什么？
============================================


S：
——————————————————————————————————————————————————————————————————
impl KvStore {
    pub fn set(&self, key: String, value: String) { // 
        unimplemented!("set is not implemented!");
    }
}

impl KvStore {
    pub fn set(self, key: String, value: String) { // 
        unimplemented!("set is not implemented!");
    }
}
——————————————————————————————————————————————————————————————
Q：
不是很能理解所有权转移到类成员方法的参数中这一设定的意义
=============================================


// TODO: 再学一学lifetime specifier
// TODO: 理解为什么traits中可能出现lifetime specifier，以及如果出现了该怎么处理
记一下as_mut的用法以及原因，应该在收藏夹里


以下代码中
    match bincode::deserialize_from::<_, R>(&mut cursor)

泛型实际被推导为
    match bincode::deserialize_from::<&mut std::io::Cursor<&[u8]>, R>(&mut cursor)

由于mut也成为了推导的一部分，可知mut也是类型系统的一部分，mut Cursor也实现了std::io::Read


mut borrow get_mut move copy
borrow并不导致所有权转移，仅仅是借用，所以&mut self之间是可以赋值的

所有权系统貌似比我想象的还要复杂不少，所有权的检查同时发生在编译时和运行时


Send Sync
几个可以确定的东西：
1）rust仅通过lifetime和ownership机制解决了data race，并没有解决concurrency problem。Send和Sync这两个marker也仅是用于解决data race问题的
2）一旦对象的ownership归属于某个进程时，唯有该进程能够访问这个对象
3）炸panic不是大问题，死锁不是大问题，程序进入未定义状态并继续运行才是真正恐怖的大问题

推论：保证所有权交接的data race safety，就能保证对象的线程安全？

    If T: Send, then passing by-value a value of type T into another thread will not lead to data races (or other unsafety)
    If T: Sync, then passing a reference &T to a value of type T into another thread will not lead to data races (or other unsafety) (aka, T: Sync implies &T: Send)

https://huonw.github.io/blog/2015/02/some-notes-on-send-and-sync/

*The mutable reference type has the guarantee that it is globally unaliased, so if a thread has access to a piece of data via a &mut, then it is the only thread in the whole program that can legally read from/write to that data. In particular, there’s no sharing and there cannot be several threads concurrently accessing that memory at the same time, hence the sharing-related guarantees of Sync don’t guarantee thread-safety for &mut. (RFC 458 describes this as &mut linearizing access to its contents.)*


*The general rule is that transferring a &mut T between threads is guaranteed to be safe if T: Send, so &mut T behaves very much like T with relation to concurrency.*
这个safe比较模糊，是data race safety还是concurrency safety？

如何在多个线程中共享&mut T?
Sync只能实现安全共享&T，估计要用内部可变性来实现
Arc<RefCell<T>> ? 恐怕不行，Arc仅能保证指针引用计数无data-race，无法保证RefCell的thread-safety
Arc<Mutex<T>> ? 底层OS语义保证所有权移交不产生冲突，所有权移交完成后内部可变性也成为了完全的安全行为

如果用C++，那么把writable换成缓冲池，采用offset读旧行了，非常安全。但在rust中，访问对象必须有（或者借用）这个对象的所有权，即使通过内部可变性也无法避免borrow checker爆出panic
综上考虑，决定用memtable + sstable，其中memtable使用无锁数据结构，外面嵌套协议实现WW阻塞和WR安全

问题：mmap接口是否能保证thread-safe？memmap::Mmap并没有实现Sync Trait。如果这个wrapper也采用lazy实现的话可能会出大问题
使用mmap作为只读buffer应该是没问题的。多线程共享页表，在data page被映射到内存前，内核可以保证对应的data page无法被访问到。

总体思路：
Arc<T>实现了Send，则 & immut Arc<T>实现了Sync，使得我们不需要关心T内部对象是否需要Send/Sync。为T设计内部可变性的接口，使用原子操作/小粒度锁保证内部的约束一致性。
不使用锁的话去维护多个变量间的约束一致性很困难，不如将状态转换为用单个变量的原子操作；

    struct Memtable {
        content: lockfree:HashMap<Key, Data>;
        uncompacted: AtomicU64,
    }

    thread_read_write() {
        atomic {
            if IsCompacting == CheckState() {               // 也许可以用CAS完成，设定符号位为is_compacting就行了
                return ErrAgain
            } else {
                pin_count+= 1;
            }
        }

        do something;

        atomic {
            size += len;
            if size > compact_limit {
                SetState(SHOULD_COMPACT);
            }
            pin_count -= 1;
        }
    }

    thread_compact() {
        atomic {
            if pin_count > 0 || is_compacting  {
                return Ignore
            }
        }

        do compaction;

        atomic {
            clear();
            SetState(OK);
        }
    }


    thread_read_write() {
        let mut retry = 10;
        loop {
            if 0 == retry {
                return ErrWait;
            }
            let pin = self.pin_count.fetch();
            if pin > COMPACT-PINCOUNT {  // 需要在外部设置线程数远小于COMPACT-PINCOUNT
                return ErrWait
            }
            if CAS(&self.pin_count, pin, pin + 1) {
                break;  // 我们成功获取了“锁”
            } else {
                // nothing todo, return to loop
            }
            retry -= 1;
        }

        // do something

        self.size.fetch_add(len);
        if self.size >= compact_limit {
            loop {
                me = self.pin_count.fetch();
                if me > COMPACT-PINCOUNT ||  // 已经有线程将状态设置为了COMPACT
                    CAS(&self.pin_count, me, me + COMPACT_PINCOUNT)     // 自己将状态设置为了COMPACT
                {
                    break;
                }
            }
        }

        pin_count.fetch_add(-1);
    }

    thread_compact() {
        loop {
            let pin = self.pin_count.fetch();
            if pin > COMPACTING-TAG || pin < COMPACT-TAG {
                break;
            }

            if !CAS(&self.pin_count, pin, pin + COMPACT-TAG) {
                // nothing to do, continue wait;
            } else {
                // 由本线程负责压缩，需要等待pin_count降到0
                loop {
                    if 0 == self.pin_count.fetch() {
                        break;
                    }
                }
                do_compaction();
                self.clear();
                // 逻辑运行到这里时，应当满足self.pin_count等于COMPACT-TAG
                assert(CAS(self.pin_count, COMPACT-TAG, 0));
            }
        }
    }

>https://doc.rust-lang.org/nomicon/send-and-sync.html</br>
Send and Sync are also automatically derived traits. This means that, unlike every other trait, if a type is composed entirely of Send or Sync types, then it is Send or Sync.

这个实现有点费解，就像是一组事务不可能直接简单组合成分布式事务一样，以后再探究吧

project 的目的应该是指导实现安全的内部可变性

实际上就是自己实现了一把RWLock，实际上性能估计比不上挂上RWLock高

经验：解析符号表是个非常重的操作，把返回值由failure改成了普通的enum，测试时间从12~13s缩短到了8s左右，所以不要随便返回error

将所有data members均用Arc包裹起来后，KvStore本身就是Clone和Send的，通过内部可见性可以实现多线程读写，现在要把borrow check的工作交给我们了

    pub struct KvStore: KvsEngine {
        manifest: Arc<Mutex<Manifest>>,
        sstables: Arc<ThreadSafeMap<file_id, KVSSTable>>,
        memtable: Arc<Memtable>,
    }

    pub fn set(&self, key, value) {
        self.memtable.set(key, value);
    }

    pub fn get(&self, key) {
        self.memtable.rlock_phony();
        defer memtable.runlock_phony();

        if let Some(fid: file_id, r: Either<Pos, Data>) = self.memtable.get(key) {
            if r.(Data) {
                return deserialize(r.Data)
            } else {
                return read_from_sstable(fid, r.(Pos));   // 这里要保证读取SSTable的Pos时，文件仍然存在，因此用了粗粒度的锁，等待读完SSTable后再释放memtable的rlock
            }
        } else {
            return KeyNotFound
        }
    }

    pub fn remove(&self, key) {
        self.memtable.remove(key);
    }

    pub fn dump(&self) {
        if !self.memtable.should_compact() {
            return;
        }

        self.memtable.wlock_phony();
        let records = self.memtable.get_all_inmem();
        dump records to file
        reset corresponding file_id and Pos by interior mutability interface
        self.memtable.wunlock_phony();
    }

上面的设计其实比较投机取巧，只不过是在一个lockfree数据结构上实现了一个简单的自旋锁而已。虽然project让实现一个lock-free reader，且read/write也真正做到了互不阻塞，但思路最终也还是回到了锁上。
本质上讲，我们需要锁的核心原因仍然是要保证结构体成员间的invariants在并发环境下仍然能够维持，因此即使引入了lockfree map，仍然会因为要维持这个map与结构体内其他成员在并发环境下的invariants而回到锁的思路上。

能够做到更好地设计？我想不出除了类锁语义外更好地方案；实际上，锁的性能并没有我们想象的那样差。内核提供的FUTEX已经很快了，低竞态下几乎不会进入内核态，仅仅是原子操作。


为什么要拆分成ImmuFile和MutFile？
> Arc<T> will implement Send and Sync as long as the T implements Send and Sync. Why can’t you put a non-thread-safe type T in an Arc<T> to make it thread-safe? This may be a bit counter-intuitive at first: after all, isn’t the point of Arc<T> thread safety? The key is this: Arc<T> makes it thread safe to have multiple ownership of the same data, but it doesn’t add thread safety to its data.

为了能Send。KVSSTable实现了之前定义的Storage，但这个trait并不Send，导致包含其的hashmap也无法Send


刚刚注意到会需要两个线程池，一个是KvStore的线程池，读、写、删、压缩并行，另一个是KvServer的线程池，专心处理用户请求

问题：compaction是应该由engine驱动的，保证compaction的进程与读写互相并行，但是接口定义并不允许
可能的解决方案：给engine内部单独开一个compaction线程，周期执行


当初为什么要把sstable keys和memtable keys合并到了一起？
因为这对compaction和flush都非常有用。bitcast的compaction不需要访问SSTable，因为内存中已经维护了所有的keys，因此可以直接通过一轮遍历map实现compaction


怎么处理loop retry的逻辑？
fn retry_loop<F, H, C, R>(func: F) -> R
where
    F: Fn() -> H,
    H: Result<(V, MemtableState)>,
    C: Fn() -> R,
{
    unimplement!()
}

pub struct KvStore {
    inner: Arc<KvStoreInner>,
    handle: Arc<JoinHandle<()>>,
}

迭代器一般是懒加载的，所以创建迭代器后立刻求长度可能不太现实

2022.5.23：
迭代器的懒加载+浅拷贝非常好用，但也要注意一下其转换引入的开销
类型推导居然也可以渗入到内嵌类型中，这是rust编译器本身强还是说是类型推导通用的？
总算完成无锁内存读了...虽然估计bench的效果还不如有锁读

task-threadpool：

1）Send？pool应该不需要Send，没有在线程间交换任何东西
2）grateful stop
3）error handling


八股文中的线程池：
core-thread + wait-thread + 任务队列

只做core-thread就行了，任务传Box包裹的闭包
所有core-thread启动后挂机在task-channel上等任务，main-loop先把channel读端取出队列，丢任务，
core-thread完成任务后把自己的channel丢回队列

为什么会想到channel？因为传任务涉及到线程间的move，需要Send Trait，job需要被Box包裹住，core-thread要先wait，接到任务后被wakeup。用channel实现wait和接收任务，接口最为简洁清晰

main-loop也需要等待外界送任务，用阻塞队列更合适

SharedQueueThreadPool


new(nthread) {
    task_queue = BlockQueue::with_capacity(nthread);

    for _ in 0..nthread {
        sender, receiver = create_spsc_channel();

        let handle = thread::spawn(move || {
            let sender = sender.clone();
            loop {
                let job = receiver.listen();
                job();
                // notify garbage collect
                senders.push(sender);
            }
        })

        handles.push(handle);
        senders(sender);
    }
}

spawn(job) {
    task_queue.push(job);
}

所有请求都pend在同一把mutex上可能不是个好主意，因为notify all后不但会引入非常大的竞态，而且还导致了一段不短的时间内请求必须串行执行，与lockfree的本意相违背。而且这段代码也存在唤醒丢失的隐患，被迫引入了timeout。

如果引入一个flush标志位也并不好，因为flush标志位必定要被多线程读，不采用同步语义的话难以保证flush的可见性。

不如让它们等待在不同的lock上 —— 不行：
>Note that any attempt to use multiple mutexes on the same condition variable may result in a runtime panic.

总的来说，一个condVar只能和一个mutex绑定，但如果把多个请求pend到同一个cond上的话，就要被迫接受唤醒后的强竞态

可能无法避免唤醒丢失了，只能采用超时重试的方法；
需要一种新的同步原语

spmc channel? 恐怕很难用，难以确定consumer的个数，Send阻塞了的话就全炸了，而且挨个唤醒仍然是串行的..
spsc？唤醒绝对不会丢失，但可能需要额外引入一个lockfree队列
crossbeam.Parker?
我有点担心它是自旋的，在flush和compaction下都应该睡眠才对；不过它的行为和spsc很像，应该可以用，不过得包装一下；

struct Pending {
    
}


impl ThreadPool for SharedQueueThreadPool {
    fn new(nthread: u32) -> Result<Self>
    where
        Self: Sized {
        let mut thread_handles = Vec::new();
        let senders = LockFreeQueue::new(nthread as usize);

        for _ in 0..nthread {
            let (sender, receiver) = channel::<TaskType>();
            let handle = std::thread::spawn(move || {
                loop {
                    match receiver.recv() {
                        Ok(job_wrapper) => {
                            job_wrapper();
                        }

                        Err(err) => {
                            break;
                        }
                    }
                }
            });

            thread_handles.push(handle);
            senders.push(sender).unwrap();
        }

        Ok(Self{
            thread_handles,
            task_senders: Arc::new(senders),
            cond: Condvar::new(),
            lock: Mutex::new(false),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: Send + 'static + FnOnce() {
        let sender = self.task_senders.pop().unwrap();
        let sender2 = sender.clone();


catch_unwind可以用在job内部，这样线程不会因为panic而丢失

### tokio异步编程
pin 是啥，看到好多次了，理解不能

总算找到专业名词了，**惊群现象**

### tokio同步原语
Mutex、RWMutex，基于标准库，不推荐采用
Channel，种类繁多，不用担心异步下的data race，推荐使用
Barrier，屏障，感觉目前用途不大
Semaphore，信号灯，设定最大异步并发数
Notify，类似于Semaphore = 1的信号灯，我居然忘了有这个同步原语！所有线程必定是阻塞在同一个wait_queue上的！

### tokio异步网络库
有了await关键字后，感觉几乎和同步编程一样了

### 如何实现异步？为什么要异步？哪些操作可以异步？
结合下bule store？

##### why futures? networking vs file/io, blocking vs non-blocking, sync vs async


##### futures from a user persective (not a poll-centric implementation perspective)
don't think too hard about executors and runtimes
method chaining and how it transforms the future type
debugging Rust types
Result vs Future vs FutureResult
error handling with futures
concrete futures vs boxed futures vs anonymous futures
note about futures 0.1 and futures 0.3 (we'll use futures 0.1)
note about async / await

### 项目设计

##### KvStore要自带线程池
wtf？不应该是KvServer自带线程池吗？

    The key/value engine that reads and writes to files will remain 
    synchronous, scheduling work on an underlying thread pool, 
    while presenting an asynchronous interface. Along the way you will
    experiment with multiple ways of defining and working with future types.

    Your KvsServer will be based on the tokio runtime, which handles the
    distribution of asynchronous work to multiple threads on its own 
    (tokio itself contains a thread pool). This means that your architecture 
    will actually have two layers of thread pools: the first handling with 
    the networking, asynchronously, one thread per core; the second handling
    the file I/O, synchronously, with enough threads to keep the networking
    threads as busy as possible.


KvStore的线程池负责读/写/压缩，以充分应用多线程优势，本身是同步过程但要提供异步接口？为什么要提供异步接口？还是因为这可能是一个长任务，需要await切换。虽然我的实现是基于mmap的，但由于mmap的
懒加载机制，page fault和swap仍然可能陷入内核，不可忽视。mmap真的不如缓冲池，不能精准控制内存还是有点要命的。
KvServer的线程池承接客户端请求，对于engine去spawn一个任务就行了。

#### KvsClient使用异步IO，并变成一个future type（boxed future、explicit future、anonymous future）
第一步不难，一个Cli就是一个单连接而已，把网络库换成tokio的net就行
第二步要把仅是一个bin的kvs-client设计成一个真正的Client类，并提供对应的API

    Client::get(&mut self, key: String) -> Box<Future<Item = Option<String>, Error = Error>
    Client::get(&mut self, key: String) -> future::SomeExplicitCombinator<...>
    Client::get(&mut self, key: String) -> impl Future<Item = Option<String>, Error = Error>
    Client::get(&mut self, key: String) -> ClientGetFuture

**untenable** 是啥？

##### ThreadPool需要sharable？KvsEngine需要变成future?
第二步是因为需要用await实现协程访问
第一步是因为KvServer需要使用线程池去spawn新的读/写任务

##### 把KvsEngine丢到tokio runtime里面去



##### 关于惊群效应的一些解决思路
Semaphore被deprecate了，

伪代码：
    consumer:
        std::this_thread::wait_for(&cond, longest_dur);

    producer:
        std::this_thread::notify_all(&cond);


不能用锁，lock必定不合语义
Park看似诱人但仍然不能用，因为Parker和Unparker均只能Send不能Sync，且Parker不能Clone。换句话说，Park看似是一对一的原语，不适用于wake up all的语义。另一点，即使使用一对一的语义，把Unparker收集起来，也要给每个阻塞的任务均Unpark一遍，如果涉及到系统调用的话，开销估计不比简单的使用CondVar触发的惊群效应低。更不用说Park的实现看起来像是自旋实现。

https://man7.org/linux/man-pages/man7/sem_overview.7.html
系统调用的Semphore也因为接口（增1/减1）而不能直接使用


XDM，我用条件变量的时候遇到了个场景。标准库要求使用CondVar的时候需要用一个Mutex去保证唤醒不会丢失，类似于这样：

bool start = false;
pthread_mutex_t mu;
pthread_cond_t cond;

thread th = [&mu, &cond]{
    std::time::sleep(1);
    {
        MutexGuard guard = mu.lock();
        start = true;
    }
    cond.notify_all();
};

pthread_start(th);

mu.lock();
while (!start) {
    cond.wait(&mu);
    mu.lock();
}

这样可能会在唤醒的时候触发惊群效应，导致大量的线程刚刚被唤醒又因为竞争mu被迫阻塞了。正常来说这样开销是无需理会的，但在我的情景里，

条件变量可以做到，但条件变量必须配合Mutex一起使用保证唤醒不丢失，这可能会导致notify_all的时候竞争Mutex降低性能，这个损失我不太想接受，因为我只想要一个等待-唤醒的语义

最终决定使用busy loop，适时调整sleep time吧，不想自己搓轮子。


2022-06-11
Client基本完工了，接下来是准备Server和Engine
个人猜测异步不会使Engine的性能得到什么提升，因为基本都是内存操作，没有用到任何异步IO库，且page fault会阻塞所有的进度...
Server和Engine需要两套thread-pool、两套runtime...这些thread间要怎么交互呢？回调 or channel?
有时间仿照leveldb试试双缓冲？不过得先去perf看一下memtable的诡异嗲坡度，居然是内存操作最浪费时间？

思考了一下，也许能做到全lock free？不需要pin了，flush-pin的时候只需要sleep一下就行？
不行，必须保证从memtable中取到rid之后，rid对应的文件一直活着，可以修改一下文件的定义，去这样做：

    loop {
        match self.memtable.get(key) {
            None => {
                return Ok(None)
            }

            MemtableValue::ValueStr(value) => {
                return Ok(Some(value))
            }

            MemtableValue::RID(rid) => {
                let pin_guard = self.pin_file(rid);
                if pin_guard.is_none() {
                    continue;
                }
                // rid对应的SSTable已经被pin住，不会被bg_compact线程给删除掉
                self.read_from_sstable(rid);
                // pin_guard被drop，对应SSTable的引用计数减1
            }
        }
    }

retry-until-success的确是lock-free编程里很好用的一套方案


重新思考下俩池子的设计，就假装那是个异步的IO接口
感觉并不是开个池子，而是开两个runtime，一个runtime不停的spawn handle_conn，另一个runtime不停地spawn engine？

2022-06-12
不知道crossbeam中的mpmc能不能保证公平丢任务，如果一个线程在做任务，但有一个任务丢到了它身上，就拉长了平均等待时间

.await时，实际上是对一个future进行了poll_ready；如果poll_ready失败，那么future可能需要被move给其他线程。又由于上述关系，future必须保留await点时的上下文信息（例如栈/堆上的变量）。总而言之，**只有future中所有的上下文均是Send的，那么future才是Send的。如果future不Send，就不能被异步实现**。
AsyncKvServer必须是Sync的。因为AsyncKvServer提供的是异步函数接口，借用了&self，在这里await时会强制要求&self可以Move，即要求AsyncKvServer必须Sync。


后面要做的事：
打Bench，绘制火焰图，看看速度慢的原因；
双缓冲 + delay delete，回顾一下leveldb的双缓冲是怎么实现的；
看看lockfree::Map的源码，了解一下lockfree是怎么实现的；
将serialize、deserialize的过程也实现shallow copy
了解一下tokio的实现，顺便加深一下lifetime specifier的理解


写成blog，把知识固化下来

2022-06-14
线程池要求函数是FnOnce + Send + static'。前两个好理解，第三个是因为必须保证闭包捕获的所有成员合法，因为线程根本不知道什么时候会创建、什么时候会调度。

闭包的生命周期应该是只与其捕获的变量有关，或者内部触发了更加精确的条件。
the parameter type `R` may not live long enough
...so that the type `[closure@src/engines/kv/async_store.rs:50:25: 53:10]` will meet its required lifetime bounds...
注意到是type may not live long enough，不是variable


engine独立配置线程池接收读/写任务，与多线程直接访问engine，哪个好？
对于这个项目来说，我感觉后者远好于前者，因为读的内容非常少，每次读完都需要额外引入一次唤醒操作，这个操作的消耗大概率能抵消掉专用读线程指令/数据cache所带来的优势。

如果希望使用异步IO带来的优势，按照我的推论，异步IO需要尽可能做到不陷入内核。
又开始迷惑了，感觉说不出异步的优势了


2022-06-15
老问题，自身实现了clone trait则难以实现drop trait。或者说，如何实现shallow destruct 和 deep destruct.
解决方案已经有了：
问题描述：由于线程的调度时机是不可知的，因此闭包捕获变量时，要么获得其所有权，要么要求变量的生命周期为'static，要么要使用“共享所有权”类语义。

    #[derive(Clone)]
    pub struct KvStore {
        inner: Arc<KvStoreInner>,
    }

    impl Drop for KvStore {

    }


在原项目中，每个KvStore都会spawn一个后台线程，这个线程会周期调用KvStoreInner中的一个`bg_flush_compaction_loop()`函数。根据前文分析可知，为实现这一点，要将KvStoreInner包裹在Arc下，然后move给后台线程，类似如下代码：

    pub fn new() -> KvStore {
        let inner = Arc::new(KvStoreInner::new());
        let inner2 = inner.clone();
        std::thread::spawn(move || inner2.bg_flush_compaction_loop());
        return KvStore { inner }
    }

    impl Drop for KvStore {
        fn drop(&mut self) {
            ...
        }
    }

    // shallow copy
    impl Clone for KvStore {
        fn clone(&self) -> Self {
            ...
        }
    }

这样，每个inner的引用计数至少是2。在显式drop KvStore时，inner的引用计数只会降至1，无法完成inner中资源的清理。一种思路可能是在KvStore被Drop时，先唤醒后台线程并等待后台线程退出，使inner的引用计数降至1，这样待KvStore的Drop函数执行完毕后，KvStoreInner的Drop就会被执行：
    impl Drop for KvStore {
        fn drop(&mut self) {
            wake up background thread and wait for background thread exit...
        }
    }

上述策略同样不能采用，因为KvStore有Clone Trait，且KvStore是Shallow Copy，这样每次Drop一个KvStore时，无法判断是否有其他线程还在使用KvStoreInner，如果有的话，就必须保留后台线程。

这里我在KvStore中引入了一个DropGuard，这使得DropGuard的引用计数与KvStore的Shallow Copy个数一致。当最后一个KvStore的Shallow Copy被drop时，会触发DropGuard的drop函数，在这个drop函数中杀掉后台线程是安全的，因为此时的KvStore是无法访问的。
