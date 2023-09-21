#[macro_export]
macro_rules! virtual_bench_main {
    ( $( $func_name:ident ),+ $(,)* ) => {
        fn main() {

            let benchmarks : &[&(&'static str, fn()->u128)]= &[

                $(
                    &(stringify!($func_name), $func_name),
                )+
            ];

            $crate::runner(benchmarks);
        }
    }
}
