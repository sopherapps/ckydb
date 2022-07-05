use ckydb::{connect, Controller};

fn main() {
    let mut db = connect("db", 4.0, 60.0).unwrap();
    let records = [
        ("hey", "English"),
        ("hi", "English"),
        ("salut", "French"),
        ("bonjour", "French"),
        ("hola", "Spanish"),
        ("oi", "Portuguese"),
        ("mulimuta", "Runyoro"),
    ];
    let updates = [
        ("hey", "Jane"),
        ("hi", "John"),
        ("hola", "Santos"),
        ("oi", "Ronaldo"),
        ("mulimuta", "Aliguma"),
    ];

    // Setting the values
    println!("[Inserting key-value pairs]");
    for (k, v) in &records {
        let _ = db.set(*k, *v);
    }

    // Getting the values
    println!("\nAfter inserting keys");
    println!("===============");
    for (k, _) in &records {
        let got = db.get(*k).unwrap();
        println!("For key: {:?}, Got: {:?}", *k, got);
    }

    // Updating the values
    for (k, v) in &updates {
        let _ = db.set(*k, *v);
    }

    println!("\nAfter updating keys");
    println!("===============");
    for (k, _) in &records {
        let got = db.get(*k).unwrap();
        println!("For key: {:?}, Got: {:?}", *k, got);
    }

    // Deleting some values
    let keys_to_delete = ["oi", "hi"];
    for k in keys_to_delete {
        let removed = db.delete(k);
        println!("Removed: key: {:?}, resp: {:?}", k, removed);
    }

    println!("\nAfter deleting keys: {:?}", keys_to_delete);
    println!("===============");
    for (k, _) in &records {
        let got = db.get(*k);
        println!("key: {:?}, Got: {:?}", *k, got);
    }

    // Deleting all values
    let cleared = db.clear();
    println!("Cleared: {:?}", cleared);

    println!("\nAfter clearing");
    println!("===============");
    for (k, _) in &records {
        let got = db.get(*k);
        println!("key: {:?}, Got: {:?}", *k, got);
    }

    db.close().expect("close");
}
