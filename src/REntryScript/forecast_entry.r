# R bridge for the v2 Python parallel entry script (entry.py).
# Sources the unchanged forecast.r and runs one mini-batch CSV through
# entry_script$run, writing the resulting forecast lines to the output file.
# Args: 1=input csv, 2=output file, 3=forecast.r path.
args <- commandArgs(trailingOnly = TRUE)
source(args[[3]], chdir = TRUE)
# Let read.csv infer types so DATEKEY/TRANSACTIONQTY are numeric: forecast.r calls
# seq()/order() on DATEKEY without converting it (R's as.character drops any ".0").
mb <- read.csv(args[[1]], check.names = FALSE, stringsAsFactors = FALSE)
if (!is.null(entry_script$init)) entry_script$init()
writeLines(as.character(entry_script$run(mb)), args[[2]], useBytes = TRUE)
