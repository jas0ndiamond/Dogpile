#!/bin/bash

echo "===================================================\nRunning init script\n==========================="

#db.createCollection('receipts');
#db.createCollection('documents');
#db.createCollection('invoices');


mongo -- "dogpile_work" <<EOF

dbAdmin = db.getSiblingDB('admin');

// create user
dbAdmin.createUser({
  user: 'dogpile',
  pwd: 'dogpile_pass',
  roles: [{ role: 'readWrite', db: 'dogpile_work' }],
});

// Authenticate user
dbAdmin.auth({
  user: "dogpile",
  pwd: "dogpile_pass",
  mechanisms: ["SCRAM-SHA-1"],
  digestPassword: true,
});

// create dispy work cache
db = new Mongo().getDB("dogpile_work");

// applications create their own collections

EOF
