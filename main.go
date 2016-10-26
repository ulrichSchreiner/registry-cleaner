package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/docker/distribution"
	dockercontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/client"

	_ "github.com/docker/distribution/manifest/schema1"
	_ "github.com/docker/distribution/manifest/schema2"
)

type blobinfo struct {
	repo    string
	tag     string
	digest  string
	created time.Time
}

type repository struct {
	ctx           context.Context
	repourl       string
	reponame      string
	repo          distribution.Repository
	tags          distribution.TagService
	blobs         distribution.BlobStore
	manifests     distribution.ManifestService
	digestConfigs map[string]interface{}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func getAllRepos(ctx context.Context, reg client.Registry) []string {
	var repos []string
	for {
		reps := make([]string, 10)
		_, err := reg.Repositories(ctx, reps, "")
		for _, r := range reps {
			if r != "" {
				repos = append(repos, r)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
	}
	return repos
}

func getRepository(ctx context.Context, repourl, repname string) (*repository, error) {
	name, _ := reference.ParseNamed(repname)
	rep, err := client.NewRepository(ctx, name, repourl, transport)
	if err != nil {
		return nil, err
	}
	tgs := rep.Tags(ctx)
	blobs := rep.Blobs(ctx)
	mfs, err := rep.Manifests(ctx)
	if err != nil {
		return nil, err
	}
	return &repository{
		ctx:           ctx,
		repourl:       repourl,
		reponame:      repname,
		repo:          rep,
		blobs:         blobs,
		tags:          tgs,
		manifests:     mfs,
		digestConfigs: make(map[string]interface{}),
	}, nil
}

func (r *repository) getConfig(dig digest.Digest) (map[string]interface{}, error) {
	mf, err := r.manifests.Get(r.ctx, dig)
	if err != nil {
		return nil, err
	}
	_, pl, err := mf.Payload()
	if err != nil {
		return nil, err
	}
	plmap := make(map[string]interface{})
	json.Unmarshal(pl, &plmap)
	cfg := plmap["config"].(map[string]interface{})
	digs := cfg["digest"].(string)
	pl, err = r.blobs.Get(r.ctx, digest.Digest(digs))
	if err != nil {
		return nil, err
	}
	plmap = make(map[string]interface{})
	err = json.Unmarshal(pl, &plmap)
	return plmap, err
}

func (r *repository) getBlobInfos() ([]blobinfo, error) {
	var result []blobinfo

	all, err := r.tags.All(r.ctx)
	if err != nil {
		return nil, err
	}

	for _, t := range all {
		tg, e := r.tags.Get(r.ctx, t)
		if e != nil {
			log.Printf("ERROR: cannot get tag info for tag '%s': %s", t, e)
			continue
		}
		u, e := r.getConfig(tg.Digest)
		if e != nil {
			log.Printf("ERROR: cannot get config for digest '%s': %s", tg.Digest, e)
			continue
		}
		tm, e := time.Parse(time.RFC3339Nano, u["created"].(string))
		if e != nil {
			log.Printf("ERROR: cannot parse created timestamp as RFC3339Nano '%s': %s", u["created"], e)
			continue
		}
		bi := blobinfo{
			tag:     t,
			repo:    r.reponame,
			digest:  string(tg.Digest),
			created: tm,
		}
		result = append(result, bi)
	}

	return result, nil
}

var (
	user      = flag.String("user", "", "the user to login for your registry")
	password  = flag.String("password", "", "the password to login for your registry")
	numDays   = flag.Int("num", -1, "number of days to keep; keep negative when you want to dump the digest's")
	dry       = flag.Bool("dry", true, "do not really delete")
	transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
)

func main() {
	flag.Parse()
	registryURL := flag.Arg(0)
	if registryURL == "" {
		fmt.Printf("Specify a registry URL")
		os.Exit(0)
	}
	ctx := dockercontext.Background()
	if *user != "" {
		u, e := url.Parse(registryURL)
		checkErr(e)
		u.User = url.UserPassword(*user, *password)
		registryURL = u.String()
	}

	oldest := time.Now().Add(time.Duration(*numDays) * -24 * time.Hour)

	reg, err := client.NewRegistry(ctx, registryURL, transport)
	checkErr(err)
	repos := getAllRepos(ctx, reg)

	for _, r := range repos {
		rep, e := getRepository(ctx, registryURL, r)
		checkErr(e)
		blobs, e := rep.getBlobInfos()
		checkErr(e)
		for _, b := range blobs {
			if *numDays >= 0 {
				if b.created.Before(oldest) {
					if *dry {
						fmt.Printf("DRY: repo:%s:%s, digest: %s, created: %s\n", b.repo, b.tag, b.digest, b.created.Format(time.RFC3339))
					} else {
						fmt.Printf("repo:%s:%s, digest: %s, created: %s\n", b.repo, b.tag, b.digest, b.created.Format(time.RFC3339))
						rep.manifests.Delete(rep.ctx, digest.Digest(b.digest))
					}
				}
			} else {
				fmt.Printf("FOUND: repo:%s:%s, digest: %s, created: %s\n", b.repo, b.tag, b.digest, b.created.Format(time.RFC3339))
			}
		}
	}
}
