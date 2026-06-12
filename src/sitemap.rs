use crate::crawler::{ContentType, DiscoveredLink};
use anyhow::Result;
use chrono::Utc;
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::Path;

pub fn merge_and_save_sitemap(filepath: &str, new_links: &[DiscoveredLink]) -> Result<()> {
    let mut existing_urls: HashMap<String, (Option<String>, Option<String>, Option<String>)> =
        HashMap::new();

    if Path::new(filepath).exists() {
        let xml_content = fs::read_to_string(filepath)?;
        let mut pos = 0;
        while let Some(start) = xml_content[pos..].find("<url>") {
            let abs_start = pos + start;
            if let Some(end) = xml_content[abs_start..].find("</url>") {
                let block = &xml_content[abs_start..abs_start + end + 6];
                let loc = extract_tag(block, "loc");
                let lastmod = extract_tag(block, "lastmod");
                let changefreq = extract_tag(block, "changefreq");
                let priority = extract_tag(block, "priority");
                if let Some(loc) = loc {
                    existing_urls.insert(loc, (lastmod, changefreq, priority));
                }
                pos = abs_start + end + 6;
            } else {
                break;
            }
        }
    }

    for link in new_links {
        if link.status_code == 200
            && (link.content_type == ContentType::Html || link.content_type == ContentType::Document)
        {
            let loc = link.url.clone();
            let lastmod = Some(
                link.last_modified
                    .clone()
                    .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string()),
            );
            let changefreq = Some(match link.depth {
                0 => "daily",
                1..=2 => "weekly",
                _ => "monthly",
            }
            .to_string());
            let priority = Some(match link.depth {
                0 => "1.0",
                1 => "0.8",
                2 => "0.6",
                _ => "0.4",
            }
            .to_string());

            existing_urls.insert(loc, (lastmod, changefreq, priority));
        }
    }

    let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;
    writer.write_event(Event::Start(
        BytesStart::new("urlset").with_attributes(vec![(
            "xmlns",
            "http://www.sitemaps.org/schemas/sitemap/0.9",
        )]),
    ))?;

    for (loc, (lastmod, changefreq, priority)) in &existing_urls {
        writer.write_event(Event::Start(BytesStart::new("url")))?;

        writer.write_event(Event::Start(BytesStart::new("loc")))?;
        writer.write_event(Event::Text(BytesText::new(loc)))?;
        writer.write_event(Event::End(BytesEnd::new("loc")))?;

        if let Some(lm) = lastmod {
            writer.write_event(Event::Start(BytesStart::new("lastmod")))?;
            writer.write_event(Event::Text(BytesText::new(lm)))?;
            writer.write_event(Event::End(BytesEnd::new("lastmod")))?;
        }
        if let Some(cf) = changefreq {
            writer.write_event(Event::Start(BytesStart::new("changefreq")))?;
            writer.write_event(Event::Text(BytesText::new(cf)))?;
            writer.write_event(Event::End(BytesEnd::new("changefreq")))?;
        }
        if let Some(pr) = priority {
            writer.write_event(Event::Start(BytesStart::new("priority")))?;
            writer.write_event(Event::Text(BytesText::new(pr)))?;
            writer.write_event(Event::End(BytesEnd::new("priority")))?;
        }

        writer.write_event(Event::End(BytesEnd::new("url")))?;
    }

    writer.write_event(Event::End(BytesEnd::new("urlset")))?;
    let result = writer.into_inner().into_inner();
    fs::write(filepath, result)?;
    Ok(())
}

pub fn generate_sitemap_xml(links: &[DiscoveredLink]) -> String {
    let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None))).unwrap();
    writer.write_event(Event::Start(
        BytesStart::new("urlset").with_attributes(vec![(
            "xmlns",
            "http://www.sitemaps.org/schemas/sitemap/0.9",
        )]),
    )).unwrap();

    for link in links {
        if link.status_code == 200
            && (link.content_type == ContentType::Html || link.content_type == ContentType::Document)
        {
            writer.write_event(Event::Start(BytesStart::new("url"))).unwrap();

            writer.write_event(Event::Start(BytesStart::new("loc"))).unwrap();
            writer.write_event(Event::Text(BytesText::new(&link.url))).unwrap();
            writer.write_event(Event::End(BytesEnd::new("loc"))).unwrap();

            if let Some(ref mod_date) = link.last_modified {
                writer.write_event(Event::Start(BytesStart::new("lastmod"))).unwrap();
                writer.write_event(Event::Text(BytesText::new(mod_date))).unwrap();
                writer.write_event(Event::End(BytesEnd::new("lastmod"))).unwrap();
            }
            let priority = match link.depth {
                0 => "1.0",
                1 => "0.8",
                2 => "0.6",
                _ => "0.4",
            };
            writer.write_event(Event::Start(BytesStart::new("priority"))).unwrap();
            writer.write_event(Event::Text(BytesText::new(priority))).unwrap();
            writer.write_event(Event::End(BytesEnd::new("priority"))).unwrap();

            writer.write_event(Event::End(BytesEnd::new("url"))).unwrap();
        }
    }
    writer.write_event(Event::End(BytesEnd::new("urlset"))).unwrap();
    String::from_utf8(writer.into_inner().into_inner()).unwrap()
}

fn extract_tag(block: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);
    if let Some(start) = block.find(&start_tag) {
        let content_start = start + start_tag.len();
        if let Some(end) = block[content_start..].find(&end_tag) {
            return Some(block[content_start..content_start + end].to_string());
        }
    }
    None
}