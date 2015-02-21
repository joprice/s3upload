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

type options struct {
	bucketName string
	src        string
	dest       string
	upload     bool
	dryRun     bool
}

const (
	s3Prefix = "s3://"
)

// Uploads a folder to an s3 bucket
// export AWS_PROFILE or use --profile flag to use specific profile. aws credentials or access key/secret env variables may also be used.
func main() {
	run(parseOptions())
}

func run(opts options) {
	bucket := bucket(opts.bucketName)
	var action func(*s3.Bucket, options) error
	if opts.upload {
		action = uploadFiles
	} else {
		action = downloadFiles
	}
	if err := action(bucket, opts); err != nil {
		log.Fatal(err)
	}
}

func parseOptions() options {
	profile := flag.String("profile", "", "aws profile")
	dryRun := flag.Bool("dry-run", false, "Outputs a description of the operations that will be run. Lists remote keys in when downloading.")

	// extend default ussage with positional argument descriptions
	originalUsage := flag.Usage
	flag.Usage = func() {
		originalUsage()
		fmt.Println("  src: file or folder to upload")
		fmt.Println("  dest: destination folder")
	}

	flag.Parse()

	argCount := flag.NArg()
	if argCount != 2 {
		usage()
	}

	setProfile(profile)

	src, dest, bucketName, upload := validatePaths(flag.Arg(0), flag.Arg(1))

	return options{
		bucketName: bucketName,
		src:        src,
		dest:       dest,
		dryRun:     *dryRun,
		upload:     upload,
	}
}

func validatePaths(src, dest string) (string, string, string, bool) {
	srcIsS3 := isS3Uri(src)
	destIsS3 := isS3Uri(dest)
	var bucketName string
	if srcIsS3 != destIsS3 {
		if srcIsS3 {
			bucketName, src = parseS3Uri(src)
			fmt.Println(src, bucketName)
		} else {
			bucketName, dest = parseS3Uri(dest)
			fmt.Println(dest, bucketName)
		}
	} else {
		fmt.Println("either source or destination should begin with the s3 protocol")
		usage()
	}
	return src, dest, bucketName, destIsS3
}

func isS3Uri(uri string) bool {
	return strings.HasPrefix(uri, s3Prefix)
}

func parseS3Uri(path string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(path, s3Prefix), "/")
	bucketName := parts[0]
	key := strings.Join(parts[1:], "/")
	return bucketName, key
}

// set env var for profile if flag is present
func setProfile(profile *string) {
	if strings.TrimSpace(*profile) != "" {
		os.Setenv("AWS_PROFILE", *profile)
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

func uploadFiles(bucket *s3.Bucket, opts options) error {
	if _, err := os.Stat(opts.src); err != nil {
		return err
	}
	// remove the parent path from src
	prefix := path.Dir(opts.src)

	return filepath.Walk(opts.src, func(filename string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			key := path.Join(opts.dest, strings.TrimPrefix(filename, prefix))
			fmt.Printf("upload '%s' -> '%s'\n", filename, key)
			if !opts.dryRun {
				return upload(bucket, key, filename)
			}
		}
		return nil
	})
}

func downloadFiles(bucket *s3.Bucket, opts options) error {
	resp, err := bucket.List(opts.src, "/", "", 0)
	if err != nil {
		return err
	}
	for _, key := range resp.Contents {
		prefix := path.Dir(opts.src)
		filename := path.Join(opts.dest, strings.TrimPrefix(key.Key, prefix))
		fmt.Printf("download %s -> %s\n", key.Key, filename)
		if !opts.dryRun {
			if err := download(bucket, key.Key, filename); err != nil {
				return err
			}
		}
	}
	for _, prefix := range resp.CommonPrefixes {
		opts.src = prefix
		if err = downloadFiles(bucket, opts); err != nil {
			return err
		}
	}
	return nil
}

func download(bucket *s3.Bucket, key, filename string) error {
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
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return bucket.Put(key, data, contentType, s3.BucketOwnerFull)
}
