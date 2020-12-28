#!/bin/bash

go test -run TestPersist1
go test -run TestPersist2
go test -run TestPersist3
go test -run TestFigure8R
go test -run TestUnreliableAgree
go test -run TestFigure8Unreliable
go test -run TestReliableChurn
go test -run TestUnreliableChurn
