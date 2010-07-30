wget http://download.freebase.com/wex/2010-07-05/freebase-wex-2010-07-05-articles.tsv.bz2
bunzip2 freebase-wex-2010-07-05-articles.tsv.bz2
awk 'BEGIN {FS = "\t"}; {print $1"\t"$2"\t"$3"\t"$5}' freebase-wex-2010-07-05-articles.tsv > articles.tsv
