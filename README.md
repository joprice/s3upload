# s3upload

### building

First [go get godep](go get github.com/tools/godep). Use `godep go install` to build.

To build for linux, see [the go docs](https://golang.org/doc/install/source) for building go for other architectures, then run

```bash
  GOOS=linux GOARCH=amd64 godep go install
```

### usage

```bash
s3upload --profile="myprofile" s3://mybucket/key file
```

`source` can be a file or a folder.

`destination` is the 'virtual folder' (key prefix) the file will be uploaded under.
