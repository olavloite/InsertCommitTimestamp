## Test Insert Commit Timestamp on Emulator

Simple test application that verifies that inserting a commit timestamp works on the emulator.

The application does the following:
1. Start the Spanner emulator in a Docker container. This requires Docker to be available on the host machine.
2. Create an instance and a database on the emulator.
3. Create a table with a column that contain a commit timestamp.
4. Insert a row with a commit timestamp to the table.
5. Query the table to verify that the row was inserted.

Run with:

```shell
dotnet run
```
