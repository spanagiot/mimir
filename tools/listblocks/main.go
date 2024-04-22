// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"syscall"
	"text/tabwriter"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/listblocks"
)

type config struct {
	bucket                     bucket.Config
	userID                     string
	showDeleted                bool
	showLabels                 bool
	showUlidTime               bool
	showSources                bool
	showParents                bool
	showCompactionLevel        bool
	showBlockSize              bool
	showStats                  bool
	splitCount                 int
	minTime                    flagext.Time
	maxTime                    flagext.Time
	dataFolder                 string
	useUlidTimeForMinTimeCheck bool
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// crate log files
	logFile, err := os.OpenFile("testbucket/logfile.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error create logfile:", err)
	}
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	logger := gokitlog.NewNopLogger()
	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.StringVar(&cfg.dataFolder, "data-folder", "./testbucket/data", "Writing temperary files to")
	flag.BoolVar(&cfg.showDeleted, "show-deleted", false, "Show deleted blocks")
	flag.BoolVar(&cfg.showLabels, "show-labels", false, "Show block labels")
	flag.BoolVar(&cfg.showUlidTime, "show-ulid-time", false, "Show time from ULID")
	flag.BoolVar(&cfg.showSources, "show-sources", false, "Show compaction sources")
	flag.BoolVar(&cfg.showParents, "show-parents", false, "Show parent blocks")
	flag.BoolVar(&cfg.showCompactionLevel, "show-compaction-level", false, "Show compaction level")
	flag.BoolVar(&cfg.showBlockSize, "show-block-size", false, "Show size of block based on details in meta.json, if available")
	flag.IntVar(&cfg.splitCount, "split-count", 0, "It not 0, shows split number that would be used for grouping blocks during split compaction")
	flag.Var(&cfg.minTime, "min-time", "If set, only blocks with MinTime >= this value are printed")
	flag.Var(&cfg.maxTime, "max-time", "If set, only blocks with MaxTime <= this value are printed")
	flag.BoolVar(&cfg.useUlidTimeForMinTimeCheck, "use-ulid-time-for-min-time-check", false, "If true, meta.json files for blocks with ULID time before min-time are not loaded. This may incorrectly skip blocks that have data from the future (minT/maxT higher than ULID).")
	flag.BoolVar(&cfg.showStats, "show-stats", false, "Show block stats (number of series, chunks, samples)")

	// Parse CLI flags.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	if cfg.userID == "" {
		log.Fatalln("no user specified")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket:", err)
	}

	loadMetasMinTime := time.Time{}
	if cfg.useUlidTimeForMinTimeCheck {
		loadMetasMinTime = time.Time(cfg.minTime)
	}

	metas, deleteMarkerDetails, noCompactMarkerDetails, err := listblocks.LoadMetaFilesAndMarkers(ctx, bkt, cfg.userID, cfg.showDeleted, loadMetasMinTime)
	if err != nil {
		log.Fatalln("failed to read block metadata:", err)
	}

	printMetas(metas, deleteMarkerDetails, noCompactMarkerDetails, cfg, bkt)
}

// nolint:errcheck
//
//goland:noinspection GoUnhandledErrorResult
func printMetas(metas map[ulid.ULID]*block.Meta, deleteMarkerDetails map[ulid.ULID]block.DeletionMark, noCompactMarkerDetails map[ulid.ULID]block.NoCompactMark, cfg config, bkt objstore.InstrumentedBucket) {
	blocks := listblocks.SortBlocks(metas)

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	// Header
	fmt.Fprintf(tabber, "Block ID\t")
	if cfg.splitCount > 0 {
		fmt.Fprintf(tabber, "Split ID\t")
	}
	if cfg.showUlidTime {
		fmt.Fprintf(tabber, "ULID Time\t")
	}
	fmt.Fprintf(tabber, "Min Time\t")
	fmt.Fprintf(tabber, "Max Time\t")
	fmt.Fprintf(tabber, "Duration\t")
	fmt.Fprintf(tabber, "No Compact\t")
	if cfg.showDeleted {
		fmt.Fprintf(tabber, "Deletion Time\t")
	}
	if cfg.showCompactionLevel {
		fmt.Fprintf(tabber, "Lvl\t")
	}
	if cfg.showBlockSize {
		fmt.Fprintf(tabber, "Size\t")
	}
	if cfg.showStats {
		fmt.Fprintf(tabber, "Series\t")
		fmt.Fprintf(tabber, "Samples\t")
		fmt.Fprintf(tabber, "Chunks\t")
	}
	if cfg.showLabels {
		fmt.Fprintf(tabber, "Labels\t")
	}
	if cfg.showSources {
		fmt.Fprintf(tabber, "Sources\t")
	}
	if cfg.showParents {
		fmt.Fprintf(tabber, "Parents\t")
	}
	fmt.Fprintln(tabber)

	var rewrite_block_total_time time.Time
	log.Printf("the blocks length is %d", len(blocks))
	list_of_blocks := make([]string, 0, len(blocks))
	for _, b := range blocks {
		list_of_blocks = append(list_of_blocks, b.ULID.String())
	}
	for i, b := range list_of_blocks {
		// if !cfg.showDeleted && deleteMarkerDetails[b.ULID].DeletionTime != 0 {
		// 	continue
		// }

		// if !time.Time(cfg.minTime).IsZero() && util.TimeFromMillis(b.MinTime).Before(time.Time(cfg.minTime)) {
		// 	continue
		// }
		// if !time.Time(cfg.maxTime).IsZero() && util.TimeFromMillis(b.MaxTime).After(time.Time(cfg.maxTime)) {
		// 	continue
		// }

		// fmt.Fprintf(tabber, "%v\t", b.ULID)
		// if cfg.splitCount > 0 {
		// 	fmt.Fprintf(tabber, "%d\t", tsdb.HashBlockID(b.ULID)%uint32(cfg.splitCount))
		// }
		// if cfg.showUlidTime {
		// 	fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(int64(b.ULID.Time())).UTC().Format(time.RFC3339))
		// }
		// fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MinTime).UTC().Format(time.RFC3339))
		// fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MaxTime).UTC().Format(time.RFC3339))
		// fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MaxTime).Sub(util.TimeFromMillis(b.MinTime)))

		// if val, ok := noCompactMarkerDetails[b.ULID]; ok {
		// 	fmt.Fprintf(tabber, "%v\t", []string{
		// 		fmt.Sprintf("Time: %s", time.Unix(val.NoCompactTime, 0).UTC().Format(time.RFC3339)),
		// 		fmt.Sprintf("Reason: %s", val.Reason)})
		// } else {
		// 	fmt.Fprintf(tabber, "\t")
		// }

		// if cfg.showDeleted {
		// 	if deleteMarkerDetails[b.ULID].DeletionTime == 0 {
		// 		fmt.Fprintf(tabber, "\t") // no deletion time.
		// 	} else {
		// 		fmt.Fprintf(tabber, "%v\t", time.Unix(deleteMarkerDetails[b.ULID].DeletionTime, 0).UTC().Format(time.RFC3339))
		// 	}
		// }

		// if cfg.showCompactionLevel {
		// 	fmt.Fprintf(tabber, "%d\t", b.Compaction.Level)
		// }

		// if cfg.showBlockSize {
		// 	fmt.Fprintf(tabber, "%s\t", listblocks.GetFormattedBlockSize(b))
		// }

		// if cfg.showStats {
		// 	fmt.Fprintf(tabber, "%d\t", b.Stats.NumSeries)
		// 	fmt.Fprintf(tabber, "%d\t", b.Stats.NumSamples)
		// 	fmt.Fprintf(tabber, "%d\t", b.Stats.NumChunks)
		// }

		// if cfg.showLabels {
		// 	if m := b.Thanos.Labels; m != nil {
		// 		fmt.Fprintf(tabber, "%s\t", labels.FromMap(b.Thanos.Labels))
		// 	} else {
		// 		fmt.Fprintf(tabber, "\t")
		// 	}
		// }

		// if cfg.showSources {
		// 	// No tab at the end.
		// 	fmt.Fprintf(tabber, "%v", b.Compaction.Sources)
		// }

		// if cfg.showParents {
		// 	var p []ulid.ULID
		// 	for _, pb := range b.Compaction.Parents {
		// 		p = append(p, pb.ULID)
		// 	}
		// 	// No tab at the end.
		// 	fmt.Fprintf(tabber, "%v", p)
		// }

		// fmt.Fprintln(tabber)

		// rewrite block with filter
		// if deleteMarkerDetails[b.ULID].DeletionTime == 0 {
		startTime := time.Now()
		new_block_uid, err := rewriteBlock(b, cfg.dataFolder)
		if err != nil {
			log.Println("Failed to write new block, the block_uid is ", b, err.Error())
			continue
		}
		endTime := time.Now()
		rewrite_block_total_time = rewrite_block_total_time.Add(endTime.Sub(startTime))
		log.Printf("Execution time of rewrite block %s to %s : %v, total time for rewrite %v", b, new_block_uid, endTime.Sub(startTime), rewrite_block_total_time)
		log.Println("the current rewrite block counter is ", i, " uid is: ", b)
		// }
	}
}

func rewriteBlock(block_uid string, data_folder string) (string, error) {
	log.Printf("the block_uid is %s, and data_folder is: %s", block_uid, data_folder)
	cmd := exec.Command("thanos", "tools", "bucket", "rewrite", "--no-dry-run", "--delete-blocks",
		"--id", block_uid,
		"--objstore.config-file", "./config/objstore_config.yml",
		"--rewrite.to-delete-config-file", "./config/matchers.yml",
		"--tmp.dir", "./testbucket/data",
	)
	// Run the command and capture its output
	output, err := cmd.CombinedOutput()

	log.Println(string(output))

	if err != nil {
		fmt.Println("COMMAND is", cmd.String(), "Error:", err.Error())
		return "", err
	}

	re := regexp.MustCompile(`new=([^ ]+)`)
	match := re.FindSubmatch(output)
	if len(match) > 1 {
		block_uid := match[1]
		return string(block_uid), nil
	}
	return "", fmt.Errorf("can't read new block id from the log")
}
