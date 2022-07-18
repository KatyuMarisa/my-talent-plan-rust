// use std::sync::{Arc, Mutex, Condvar};

// pub struct DropGuard {
//     state: Arc<Mutex<RunningState>>,
//     bg_cond: Arc<Condvar>,
// }

// impl DropGuard {
//     pub fn new(state: Arc<Mutex<RunningState>>, bg_cond: Arc<Condvar>) -> Self {
//         Self{ state, bg_cond }
//     }
// }

// impl Drop for DropGuard {
//     fn drop(&mut self) {
//         // let background thread exit.
//         {
//             let mut state_guard = self.state.lock().unwrap();
//             *state_guard = RunningState::Closed;
//             self.bg_cond.notify_one();
//         }
//         // wait for background thread complete flush.
//         let mut state_guard = self.state.lock().unwrap();
//         while *state_guard != RunningState::Exit {
//             state_guard = self.bg_cond.wait(state_guard).unwrap();
//         }
//         // all background thread exit.
//     }
// }
