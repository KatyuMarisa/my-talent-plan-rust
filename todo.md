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


if maybe reach compaction size {
    
} else {

}