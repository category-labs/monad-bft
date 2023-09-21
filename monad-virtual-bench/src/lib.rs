pub mod macros;

pub fn runner(benches: &[&(&'static str, fn() -> u128)]) {
    for (name, func) in benches.iter() {
        let result = func();

        // expected output format:
        //   test bench_fib_20 ... bench:      37,174 ns/iter (+/- 7,527)

        println!("test {} bench: {} ms/iter ", name, result);
    }
}
