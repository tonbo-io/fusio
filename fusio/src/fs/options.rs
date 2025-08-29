use fusio_core::DurabilityLevel;

#[derive(Debug)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
    // Durability-related knobs (optional; backends may ignore if unsupported)
    pub write_through: bool,
    pub bytes_per_sync: Option<u64>,
    pub sync_on_close: Option<DurabilityLevel>,
    pub dirsync_on_rename: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate: false,
            write_through: false,
            bytes_per_sync: None,
            sync_on_close: None,
            dirsync_on_rename: false,
        }
    }
}

impl OpenOptions {
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        if create {
            self.write = true;
        }
        self
    }

    pub fn truncate(mut self, truncate: bool) -> Self {
        self = self.write(true);
        self.truncate = truncate;
        self
    }

    pub fn write_through(mut self, write_through: bool) -> Self {
        self.write_through = write_through;
        self
    }

    pub fn bytes_per_sync(mut self, v: Option<u64>) -> Self {
        self.bytes_per_sync = v;
        self
    }

    pub fn sync_on_close(mut self, level: Option<DurabilityLevel>) -> Self {
        self.sync_on_close = level;
        self
    }

    pub fn dirsync_on_rename(mut self, enable: bool) -> Self {
        self.dirsync_on_rename = enable;
        self
    }
}
