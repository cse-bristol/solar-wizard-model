#!/usr/bin/env Rscript

library(rmapshaper)

json_in = ""
f <- file("stdin")
open(f)
while(length(line <- readLines(f,n=1)) > 0) {
    json_in = paste(json_in,line,sep="")
}

json_out <- ms_simplify(json_in, 
                        keep_shapes = TRUE)

write(json_out, stdout())

