package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Uploads a folder to an s3 bucket
// export AWS_PROFILE or use --profile flag to use specific profile. aws credentials or access key/secret env variables may also be used.
func main() {
	bucketName := requiredArg("bucket", "", "bucket to upload to")
	profile := flag.String("profile", "", "aws profile")
	//TODO: determine this and bucket name instead from which argument starts with s3://
	up := flag.Bool("up", false, "upload")
	down := flag.Bool("down", false, "download")
	originalUsage := flag.Usage
	flag.Usage = func() {
		originalUsage()
		fmt.Println("  src: file or folder to upload")
		fmt.Println("  dest: destination folder")
	}
	flag.Parse()
	if !*up && !*down {
		fmt.Println("Either --up or --down must be provided")
		usage()
	}
	argCount := flag.NArg()
	if argCount == 0 {
		usage()
	}
	src := flag.Arg(0)
	dest := ""
	if argCount > 1 {
		dest = flag.Arg(1)
	}
	if strings.TrimSpace(*profile) != "" {
		os.Setenv("AWS_PROFILE", *profile)
	}
	var err error
	bucket := bucket(bucketName())
	if *up {
		err = uploadFile(bucket, src, dest)
	} else {
		err = downloadFiles(bucket, src, dest)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	flag.Usage()
	os.Exit(1)
}

func requiredArg(key string, defaultValue string, desc string) func() string {
	value := flag.String(key, defaultValue, desc)
	return func() string {
		if strings.TrimSpace(*value) == "" {
			fmt.Println("value required for " + key)
			usage()
		}
		return *value
	}
}

func bucket(bucketName string) *s3.Bucket {
	auth, err := aws.GetAuth("", "")
	if err != nil {
		log.Fatal(err)
	}
	client := s3.New(auth, aws.USEast)
	return client.Bucket(bucketName)
}

func uploadFile(b *s3.Bucket, src, dest string) error {
	if _, err := os.Stat(src); err != nil {
		return err
	}
	// remove the parent path from src
	prefix := path.Dir(src)
	visit := func(filename string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			key := path.Join(dest, strings.TrimPrefix(filename, prefix))
			return upload(b, key, filename)
		}
		return nil
	}

	return filepath.Walk(src, visit)
}

func downloadFiles(bucket *s3.Bucket, src, dest string) error {
	resp, err := bucket.List(src, "/", "", 0)
	if err != nil {
		return err
	}
	for _, key := range resp.Contents {
		prefix := path.Dir(src)
		filename := path.Join(dest, strings.TrimPrefix(key.Key, prefix))
		download(bucket, key.Key, filename)
	}
	for _, prefix := range resp.CommonPrefixes {
		err = downloadFiles(bucket, prefix, dest)
		if err != nil {
			break
		}
	}
	return err
}

func download(bucket *s3.Bucket, key, filename string) error {
	fmt.Printf("download %s -> %s", key, filename)
	data, err := bucket.GetReader(key)
	if err != nil {
		return err
	}
	defer data.Close()
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)
	_, err = io.Copy(writer, data)
	return err
}

func upload(bucket *s3.Bucket, key, filename string) error {
	contentType := mime.TypeByExtension(filepath.Ext(filename))
	fmt.Printf("upload '%s' -> '%s' (%s)\n", filename, key, contentType)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return bucket.Put(key, data, contentType, s3.BucketOwnerFull)
}
