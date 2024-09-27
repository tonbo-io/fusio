#[derive(PartialEq, Eq)]
pub(crate) enum WriteMode {
    Append,
    Overwrite,
}

pub struct OpenOptions {
    pub(crate) read: bool,
    pub(crate) write: Option<WriteMode>,
    pub(crate) create: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: true,
            write: None,
            create: false,
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
}
