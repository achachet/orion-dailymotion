from utils import *

def process_sitemap(url):
    print("Parsing Sitemap %s" % (url))

    sitemap = requests.get(url)
    sitemap = gunzip_bytes_obj(sitemap.content)

    soup = BeautifulSoup(sitemap, 'lxml')
    videos = soup.find_all("url")

    #print("The number of videos are {0}".format(len(videos)))
    videos_to_publish = []
    for video in videos:
        video_url = video.findNext("loc").text
        video_lastmod = video.findNext("lastmod").text
        video_lastmod_ts = timestamp_str_to_unix(video_lastmod)
        video_xid = video_url.split('/video/')[1]
        if video_lastmod_ts>reference_timestamp:
            #print("%s : %s" % (video_xid, video_lastmod))
            videos_to_publish.append(video_xid)
            
    message_ids = publish_pubsub_messages(videos_to_publish, topic_path)
    
dm_sitemap_url = 'http://www.dailymotion.com/map-regular.xml'
reference_timestamp = timestamp_str_to_unix('2018-01-01T00:00:00+01:00')

sitemap_index = requests.get(dm_sitemap_url)

soup = BeautifulSoup(sitemap_index.text, 'lxml')
sitemaps = soup.find_all("sitemap")
sitemap_urls = [sitemap.findNext("loc").text for sitemap in sitemaps]
p = multiprocessing.Pool(5)
p.map(process_sitemap, sitemap_urls)
#for sitemap_url in sitemap_urls: process_sitemap(sitemap_url)