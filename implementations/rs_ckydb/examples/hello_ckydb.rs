use ckydb::{connect, Controller};

fn main() {
    let mut db = connect("db", 4.0, 60.0).unwrap();
    let keys = ["hey", "hi", "yoo-hoo", "bonjour"].to_vec();
    let values = ["English", "English", "Slang", "French"].to_vec();

    // Setting the values
    println!("[Inserting key-value pairs]");
    for (k, v) in keys.clone().into_iter().zip(values) {
        let _ = db.set(k, v);
    }

    // Getting the values
    println!("[After insert]");
    for k in keys.clone() {
        let got = db.get(k).unwrap();
        println!("For key: {:?}, Got: {:?}", k, got);
    }

    // Deleting some values
    for k in &keys[2..] {
        let removed = db.delete(*k);
        println!("Removed: key: {:?}, resp: {:?}", k, removed);
    }

    for k in &keys {
        let got = db.get(*k);
        println!("[After delete: For key: {:?}, Got: {:?}", k, got);
    }

    // Deleting all values
    let cleared = db.clear();
    println!("Cleared: {:?}", cleared);

    println!("[After clear]");
    for k in &keys {
        let got = db.get(*k);
        println!("For key: {:?}, Got: {:?}", k, got);
    }
    db.close();
}
