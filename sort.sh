# These are needed so that sort/uniq uses a simple byte comparison and nothing language-specific, same is needed for sort
export LC_ALL=C
export LC_COLLATE=C

# uniq -d drops singleton lines
# perl - move the count to the end of the line behind a tab
for f in $1/*
do
    echo $f
    gzip -d -c $f | sort --compress-program gzip | uniq -c -d | perl -pe 's/^\s*([0-9]+)\s+(.*)$/\2\t\1/' | sort -n -r -k 3 --field-separator=$'\t' --compress-program gzip |gzip -c > ${f%.txt.gz}.sorted_by_count.txt.gz
done



