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
    pub fn read(mut self) -> Self {
        self.read = true;
        self
    }

    pub fn write(mut self) -> Self {
        self.write = Some(WriteMode::Overwrite);
        self
    }

    pub fn create(mut self) -> Self {
        self.create = true;
        self
    }

    pub fn append(mut self) -> Self {
        self.write = Some(WriteMode::Append);
        self
    }
}
