# s3upload

### building

To build for linux, see [the go docs](https://golang.org/doc/install/source) for building go for other architectures, then run

```bash
  GOOS=linux GOARCH=amd64 godep go install
```

### usage

```bash
s3upload --bucketName "mybucket" --profile="myprofile" source destination
```

`source` can be a file or a folder.

`destination` is the 'virtual folder' (key prefix) the file will be uploaded under.
