#!/bin/bash

go test -run TestBasicAgree
go test -run TestFailAgree
go test -run TestFailNoAgree
go test -run TestConcurrentStarts
go test -run TestRejoin
go test -run TestBackup
go test -run TestCount
