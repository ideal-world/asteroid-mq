pub fn executor_digest() -> u64 {
    thread_local! {
        static MACH_ID: std::cell::OnceCell<u64> = std::cell::OnceCell::new();
    }
    MACH_ID.with(|t| {
        *t.get_or_init(|| {
            let thread = std::thread::current().id();
            let mach = machine_uid::get()
                .unwrap_or_else(|_|std::env::var("MACHINE_ID").expect("Cannot get machine id"));
            let mut hasher = std::hash::DefaultHasher::new();
            std::hash::Hash::hash(&thread, &mut hasher);
            std::hash::Hash::hash(&mach, &mut hasher);
            let digest = std::hash::Hasher::finish(&hasher);
            digest
        })
    })
}
