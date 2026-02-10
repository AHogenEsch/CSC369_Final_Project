1. What question are you trying to answer?
> Do wikipedia articles for certain politicians or political parties experience more vandalism than others? Are there spikes of vandalism when negative headlines or stories are broadcast?

2. Why is this question worth answering? This can be either because it has some personal value to you or because it is a question that others are asking. If it is a question others are asking, then provide an example.
> This question is worth answering as it could shed light on how often certain politicians are attacked, and who has the most active vandals. I am interested in finding this out because I care about our nation's politics.

3. What is your hypothesis? Why leads you towards that hypothesis?
> I think that members of the democratic party are more likely to be vandalized, as members of the conservative party (or at least accounts portraying themselves as such) are often more antaganistic on social media.

4. What are the primary datasets you will use to answer the question? The datasets must be at least 15GB in their original form when downloaded. Provide links. You may not choose r/place (any year) as your analysis set.


[Wikipedia Dumps](https://dumps.wikimedia.org/enwiki/latest/)

enwiki-latest-stub-meta-history1.xml.gz - 3.5GB compressed

enwiki-latest-stub-meta-history10.xml.gz  - 4.2GB compressed

enwiki-latest-stub-meta-history11.xml.gz - 4.2GB compressed

enwiki-latest-stub-meta-history12.xml.gz - 4.2GB compressed

enwiki-latest-stub-meta-history13.xml.gz - 4.3GB compressed

enwiki-latest-stub-meta-history14.xml.gz -4.3GB compressed


I was going to analyze weather data to find the ideal areas of california for tomato cultivation but weather data is surpringly small and tedious to download. From CIMIS I can only download 50k records at a time, and the total size of records for all their weather stations over 15 years analyzing ~10 different readings every hour was only about 1.3GB. Moreover, I would have to download roughly 310 individual csv files with their cap of 50k records, or write a script that automatically downloads, renames, and combines these files. I looked at a couple other data collection agencies but don't think I could reach the 15GB requirement. Wikipedia is much more friendly to large data analytics.

"The hourly CSV report request exceeds the maximum data limit of 50,000 records.
Your request: 118 stations * 5,480 days * 24 hours = 15,519,360 records."

