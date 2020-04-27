## Pet project using pyspark module
<b>PySparkJob.py</b><br>
From clickstream.parquet file we create train/test/validate parquet files for further training.
Clickstream.parquet structure:
<ul>
<li>date  - day when events take place </li>
<li>time - exact time of event </li>
<li>event - type of event, can be either a view or a click</li>
<li>platform -	platform, where the event occurred </li>
<li>ad_id -	ad id</li>
<li>client_union_id  - client id</li>
<li>campaign_union_id - campaign id </li>
<li>ad_cost_type - CPC or CPM type of ad </li>
<li>ad_cost - ad cost in rubles, for CPC ads is the cost per click, for CPM is the price per 1000 views </li>
<li>has_video - does the ad has a video </li>
<li>target_audience_count - the size of the audience the ad is targeted to</li>
</ul>
