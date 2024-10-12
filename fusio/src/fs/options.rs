#[derive(PartialEq, Eq)]
pub enum WriteMode {
    Append,
    Overwrite,
}

pub struct OpenOptions {
    pub read: bool,
    pub write: Option<WriteMode>,
    pub create: bool,
    pub truncate: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: true,
            write: None,
            create: false,
            truncate: false,
        }
    }
}

impl OpenOptions {
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    pub fn write(mut self, write: bool) -> Self {
        self.write = write.then_some(WriteMode::Overwrite);
        self
    }

    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    pub fn append(mut self, append: bool) -> Self {
        self.write = append.then_some(WriteMode::Append);
        self
    }

    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }
}
