#!/usr/bin/env bash

echo 'CREATE CONSTRAINT BaseIdUnique ON (b:Base) ASSERT b.id IS UNIQUE' | cypher-shell
