pub enum Command {
    Set { key: String, value: String },
    Remove { key: String, value: String },
}
