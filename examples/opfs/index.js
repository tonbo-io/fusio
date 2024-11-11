import { async_reader, async_writer, read_from_opfs, remove_all_dir, write_to_opfs } from "./pkg";

await write_to_opfs();
await read_from_opfs();

await async_writer();
await async_reader();

await remove_all_dir();
