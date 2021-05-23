#!/usr/bin/env bash

echo 'CREATE CONSTRAINT BaseIdUnique ON (b:Base) ASSET b.id IS UNIQUE' | cypher-shell
