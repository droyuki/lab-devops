cmdargs <- commandArgs()
write.table(cmdargs,file= "rLog.txt", sep = ",", col.names = NA)