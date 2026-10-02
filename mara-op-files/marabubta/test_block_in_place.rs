fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                println!("It works!");
            });
        });
    });
}
