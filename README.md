# my-talent-plan-rust

个人对 [talent-plan-rust Project-5](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-5
) 的实现。测试代码有一定修改。

### TODO List：
0. Perf + Bench
1. 优化memtable，序列化/反序列化的对象尽可能shallow copy。
2. 模仿leveldb，试试通过双缓冲避免the world效应的影响。
3. SSTable的延迟删除，彻底消除the world效应？
4. 阅读lockfree::map::Map，寻找瓶颈
5. 进一步学习tokio
