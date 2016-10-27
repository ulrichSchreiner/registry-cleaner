package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"

	log "github.com/Sirupsen/logrus"

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
	digest  digest.Digest
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
	last := ""
	for {
		reps := make([]string, 10)
		_, err := reg.Repositories(ctx, reps, last)
		for _, r := range reps {
			if r != "" {
				repos = append(repos, r)
				last = r
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

func (r *repository) getCreated(dig digest.Digest) (*time.Time, error) {

	mf, err := r.manifests.Get(r.ctx, dig)
	if err != nil {
		return nil, fmt.Errorf("cannot query manifest: %s", err)
	}
	_, pl, err := mf.Payload()
	if err != nil {
		return nil, err
	}
	plmap := make(map[string]interface{})
	json.Unmarshal(pl, &plmap)
	config := plmap["config"]
	if config == nil {
		// no config, try the first history object and use v1compatibility
		hist := plmap["history"]
		if hist == nil {
			return nil, fmt.Errorf("no config and history found for digest: %s", dig)
		}
		h := hist.([]interface{})[0]
		history := h.(map[string]interface{})
		v1compat := history["v1Compatibility"]
		if v1compat == nil {
			return nil, fmt.Errorf("no v1Compatibility node in history object")
		}
		// v1compat is no a json string, parse it
		v1comp := make(map[string]interface{})
		json.Unmarshal([]byte(v1compat.(string)), &v1comp)
		tm, e := time.Parse(time.RFC3339Nano, v1comp["created"].(string))
		return &tm, e
	}
	cfg := plmap["config"].(map[string]interface{})
	digs := cfg["digest"].(string)
	pl, err = r.blobs.Get(r.ctx, digest.Digest(digs))
	if err != nil {
		return nil, err
	}
	plmap = make(map[string]interface{})
	err = json.Unmarshal(pl, &plmap)
	tm, e := time.Parse(time.RFC3339Nano, plmap["created"].(string))
	return &tm, e
}

func (r *repository) getBlobInfos() ([]blobinfo, error) {
	var result []blobinfo

	all, err := r.tags.All(r.ctx)
	if err != nil {
		return nil, err
	}

	for _, t := range all {
		log.WithFields(log.Fields{
			"repository": r.reponame,
			"tag":        t,
		}).Info("processing tagged repository")

		tg, e := r.tags.Get(r.ctx, t)
		if e != nil {
			log.WithFields(log.Fields{
				"tag":   t,
				"error": e,
			}).Error("cannot query tag descriptor")
			continue
		}

		repname := fmt.Sprintf("%s:%s", r.reponame, t)
		if keepRepo != nil && keepRepo.FindString(repname) != "" {
			log.WithFields(log.Fields{
				"repname": r.reponame,
				"tag":     t,
				"type":    tg.MediaType,
			}).Info("keep repo which is matched by keep-regexp")
			continue
		}
		tm, e := r.getCreated(tg.Digest)
		if e != nil {
			log.WithFields(log.Fields{
				"repname":    r.reponame,
				"tag":        t,
				"descriptor": tg,
				"error":      e,
			}).Error("cannot get creation time")
			continue
		}
		log.WithFields(log.Fields{
			"repname":    r.reponame,
			"tag":        t,
			"descriptor": tg,
		}).Info("add tag info for inspection")

		bi := blobinfo{
			tag:     t,
			repo:    r.reponame,
			digest:  tg.Digest,
			created: *tm,
		}
		result = append(result, bi)
	}

	return result, nil
}

var (
	user      = flag.String("user", "", "the user to login for your registry")
	password  = flag.String("password", "", "the password to login for your registry")
	numDays   = flag.Int("num", -1, "number of days to keep; keep negative when you want to dump the digest's")
	dry       = flag.Bool("dry", false, "do not really delete")
	keep      = flag.String("keep", "", "regexp for repositories which should not be deleted, will be matched against repname:tag")
	remove    = flag.String("remove", ".*", "regexp for repositories which should be deleted, will be matched against repname:tag")
	transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	keepRepo   *regexp.Regexp
	removeRepo *regexp.Regexp
)

func main() {
	flag.Parse()
	registryURL := flag.Arg(0)
	if registryURL == "" {
		fmt.Printf("Specify a registry URL\n")
		os.Exit(0)
	}
	log.SetOutput(os.Stdout)
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	log.SetFormatter(formatter)
	if *keep != "" {
		keepRepo = regexp.MustCompile(*keep)
	}
	if *remove != "" {
		removeRepo = regexp.MustCompile(*remove)
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
	log.Info("query all repos ...")
	repos := getAllRepos(ctx, reg)

	for _, r := range repos {
		log.WithFields(log.Fields{
			"repository": r,
		}).Info("Processing")
		rep, e := getRepository(ctx, registryURL, r)
		checkErr(e)
		blobs, e := rep.getBlobInfos()
		checkErr(e)
		for _, b := range blobs {
			if *numDays >= 0 {
				if b.created.Before(oldest) {
					repname := fmt.Sprintf("%s:%s", b.repo, b.tag)
					if removeRepo != nil && removeRepo.FindString(repname) == "" {
						log.WithFields(log.Fields{
							"reponame": repname,
							"created":  b.created.Format(time.RFC3339),
						}).Info("repo is too old but not matche by remove-regexp, ignoring")
						continue
					}
					log.WithFields(log.Fields{
						"reponame": repname,
						"created":  b.created.Format(time.RFC3339),
					}).Info("repo matched for deletion")
					if *dry {
						log.WithFields(log.Fields{
							"repo":   rep.reponame,
							"digest": b.digest,
						}).Info("DRY DELETE")
					} else {
						e = rep.manifests.Delete(rep.ctx, b.digest)
						if e != nil {
							log.WithFields(log.Fields{
								"digest": b.digest,
								"error":  e,
							}).Error("error deleting digest")
						}
					}
				}
			}
		}
	}

}
