use std::error::Error;
use serde_json::Value;
use glob::glob;

use elasticsearch::{Elasticsearch, SearchParts};
use elasticsearch::http::Url;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::auth::Credentials;

// Elasticsearch information.
const ES_URL: &str = "http://archivist-es:9200";
const ES_USERNAME: &str = "elastic";
const ES_PASSWORD: &str = "password_here";

// The tubearchivist download directory.
const SOURCE_DIRECTORY: &str = "/mnt/user/tubearchivist/youtube";

// The destination directory for created symlinks.
const DEST_DIRECTORY: &str = "/mnt/user/tubearchivist/plex";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // Create ES client.
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let credentials = Credentials::Basic(ES_USERNAME.into(), ES_PASSWORD.into());
    let transport = TransportBuilder::new(conn_pool).auth(credentials).build()?;
    let client = Elasticsearch::new(transport);

    // Look up all playlists using GET "/ta_playlist/search".
    log::info!("Looking up playlists...");
    let response = client
        .search(SearchParts::Index(&["ta_playlist"]))
        .size(1000)
        .send()
        .await?;

    let response_body = response.json::<Value>().await?;

    // Process playlists in response.
    // The resulting playlists are two layers deep in fields called "hits", confusingly.
    if let Some(playlists) = response_body["hits"]["hits"].as_array() {
        for playlist in playlists {
            // Get playlist name and id and validate them.
            let playlist_name = playlist["_source"]["playlist_name"].as_str();
            let playlist_id = playlist["_source"]["playlist_id"].as_str();

            if playlist_name.is_none() {
                log::warn!("Playlist from search result has no name");
                continue;
            }

            if playlist_id.is_none() {
                log::warn!("Playlist from search result has no id");
                continue;
            }

            let playlist_name = playlist_name.unwrap();
            let playlist_id = playlist_id.unwrap();

            let symlink_dest_dir = format!("{DEST_DIRECTORY}/{} [{}]", playlist_name.replace("/", ""), playlist_id.replace("/", ""));
            log::info!("Creating symlink dest dir for playlist: {symlink_dest_dir}");
            std::fs::create_dir_all(&symlink_dest_dir)?;
            
            log::info!("Processing playlist: {} [{}]", playlist_name, playlist_id);

            // Process videos in playlist.
            if let Some(videos) = playlist["_source"]["playlist_entries"].as_array() {
                for video in videos {
                    // Get video id and title.
                    let video_id = video["youtube_id"].as_str();
                    let video_title = video["title"].as_str();
                    let video_uploader = video["uploader"].as_str();
                    let video_downloaded = video["downloaded"].as_bool().unwrap_or(false);

                    if video_id.is_none() {
                        log::warn!("Video has no name");
                        continue;
                    }

                    let video_id = video_id.unwrap();

                    if video_title.is_none() {
                        log::warn!("Video {video_id} has no title");
                        continue;
                    }

                    if video_uploader.is_none() {
                        log::warn!("Video {video_id} has no uploader");
                    }

                    let video_title = video_title.unwrap();
                    let video_uploader = video_uploader.unwrap();

                    if !video_downloaded {
                        log::info!("Skippping non-downloaded video: {video_title} [{video_id}]");
                        continue;
                    }

                    // Find video in directory.
                    log::info!("Creating symlink for: {video_title} [{video_id}]");

                    let glob_pattern = format!("{SOURCE_DIRECTORY}/{video_uploader}/*_{video_id}_*");
                    log::info!("Looking for video with glob pattern {glob_pattern}");

                    let glob_result = glob(&glob_pattern);

                    if let Ok(glob_result) = glob_result {
                        let glob_result = glob_result.into_iter().next().unwrap().unwrap();

                        let file_extension = glob_result.extension().unwrap().to_str().unwrap().to_owned();

                        let symlink_source = glob_result.into_os_string().into_string().unwrap();

                        let symlink_dest = format!("{symlink_dest_dir}/{} [{}].{}", video_title.replace("/", ""), video_id.replace("/", ""), file_extension.replace("/", ""));

                        log::info!("Creating symlink '{symlink_source}' => '{symlink_dest}'");

                        if std::path::Path::new(&symlink_dest).exists() {
                            log::info!("(already exists)");
                        }
                        else {
                            std::fs::soft_link(symlink_source, symlink_dest)?;
                        }
                    }
                    else {
                        log::warn!("Failed to find source video with glob");
                    }
                }
            }
            else {
                log::info!("No videos in playlist");
            }
        }
    }
    else {
        log::info!("No playlists in response");
    }

    Ok(())
}
