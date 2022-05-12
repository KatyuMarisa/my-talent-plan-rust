测试代码要求所有KV均在同一个目录下，且根据测试代码来看，是鼓励将kv分成多个文件的
Record Format [KeySz ValueSz Key Value]


AF内容是否有序？
===无序，因为是追加，SSTable有序那是因为它把WAL和SST分开了


压缩的实现？
===把替换文件变成一个原子操作


DBFile:
[MAGIC_NUMBER, CHECKSUM, CONTENT]


为了简便起见，先让MANIFEST仅APPEND ONLY，不考虑MANIFEST的分裂（就算考虑MANIFEST的分裂，也可能要引入一个APPEND ONLY的东西去追踪它）


MANIFEST的作用：
（1）追踪所有的DBFile
（2）实现日志压缩
可以使用WAL去实现上述需求


思路：
使用fid对DBFile进行抽象，0永远是MANIFEST；
只有完整的、可用的DBFile在MANIFEST中有fid
日志压缩的核心操作是**原子性的增添并删除一批文件**


AddOrRemove(vec[], vec[])

dbfile.rs
dbfile.rs是对数据库所使用的文件的统一抽象，DBFile一致采用如下格式：
[MAGIC_NUMBER, CRC32, [Records...]]


kvdb:


impl DBFile<RecordType> {
    pub fn open() -> Result<()>;
    pub fn drop() -> Result<()>;
    pub fn read_record_at() -> Result<Option<RecordType>>;
    pub fn records() -> Vec<RecordType>;
    pub fn write_record() -> Result<()>;
    pub fn set_read_only() -> Result<()>;
}



https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-4#user-content-part-8-lock-free-readers

Project4 要求实现无锁读，这里把几个Hints翻译一下


#### Understand and maintain sequential consistency
强调了happends before关系，核心是write后的数据必须对read可见

#### Identify immutable values
没看出什么眉目

#### Duplicate values instead of sharing
提示使用File句柄的内置并发特性实现无锁读写；或者说，多个线程共享同一个句柄（不同的File也能共享同一个句柄）；这种情况下不能直接read，因为普通的read是基于句柄的当前偏移值去读的，我们需要一种直接通过偏移值访问文件的方案，就像把文件看做是一个普通的u8数组那样。`pread`可以实现。
这个做法很简单，可以看做是使用了kernel space page buffer替代数据库广泛使用的user space page buffer。个人认为引入的代价是重复的两态拷贝。假设5个线程读了同一个page里的同一个record，则共计经历了5轮两态拷贝，代价还是不能忽视的。



需要考虑的问题：
（1）server的实现
可能需要线程池，但比较优雅的解决方式应该是多路复用，使用1~3个looper应该就足够了
开启一个thread监听连接套接字，然后分散到不同的eventLoop中（可以先只用一个eventLoop）


（2）特化的线程
实现读、写、压缩这三个线程，实现并发读写压缩
怎么实现线程间的通信？或者说，读请求怎么交给线程处理？处理完成后怎么返回？
猜测：在多路复用的场景下，多个网络连接被分不到不同的描述符上，由同一个线程处理，线程写某个套接字其实就是在回复某个连接，这也是为什么一个线程可以处理成千上万个连接的原因；
~~那么只要把描述符传给读线程，让读线程直接把结果写到描述符里就可以了！这样就解决了我比较头疼的线程间回传返回结果的问题（其实这个开销不大，毕竟线程是共享内存的，不会引入IPC开销）~~
不能直接往描述符里写，写也要回传给looper，不然写线程可能阻塞。Muduo里面的解决方案是为描述符的写可触发设定了callback



（3）无锁Read
个人的想法是用mmap实现一个无法控制容量的bufferPool，这样可以避免共享句柄和两态拷贝的问题，并发访问控制的正确性被转换为offset的有效性，在KvStorage中使用一个并发安全的HashMap保证key->(fid, offset)映射关系就好了

