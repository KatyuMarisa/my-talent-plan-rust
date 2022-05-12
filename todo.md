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