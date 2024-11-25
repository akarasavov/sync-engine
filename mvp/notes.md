USE-case 1 - be able to define a Sync rule for a single table.

### Features that needs to be implemented
- [ ] Make sure that client and server interaction works for 2..n clients for snippets table 
- [ ] Server side - algorithm of ordering of operations
- [ ] Server side - ordering of operations with optimizations for LWW - for each record leave only the final state
- [ ] Server side - integrate server side with Postgresql, be able to write and read info
- [ ] Client side - apply received operations from the server in a single transaction
- [ ] Client side - client can translate any SQL statements(without where) to tables and triggers that will be used to collect operations
- [ ] Client, Server sides - add logging
- [ ] Mercury
