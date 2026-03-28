PHONY: clean

build-%: cmd/%/main.go
	GOOS=linux CGO_ENABLED=0 go build -o build/$* cmd/$*/main.go

zip-%: build-%
	zip -j build/$*.zip build/$*

clean:
	rm build/*
