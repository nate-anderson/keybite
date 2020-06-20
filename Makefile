build-lambda:
	@echo "Building lambda archive"
	@cd ./src && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ../bin/keybite .
	@echo "Compressing Lambda executable"
	@cd ./bin && zip ./keybite.zip ./keybite
	@echo "Cleaning up"
	@rm ./bin/keybite

build-linux:
	@echo "Building Keybite"
	@cd ./src && go build -o ../bin/keybite .

build-arm:
	@echo "Building Keybite ARM"
	@cd ./src && env GOOS=linux GOARCH=arm GOARM=5 go build -o ../bin/keybite-arm .

compress-bin:
	@echo "Compressing Lambda executable"
	@zip ./bin/keybite.zip ./bin/keybite
