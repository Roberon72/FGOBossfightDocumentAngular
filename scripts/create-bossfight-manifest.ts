import { readdirSync, writeFileSync, readFileSync } from 'node:fs';
import matter from 'gray-matter';

const dir = 'raw_bossfights';
const insideDir = 'bossfights';
const publicDir = `public/${insideDir}`;

//FIXME: Metadata should be validated and typed
// to make sure it matches front-end, but I'm too lazy rn - Roberon
const markdownFiles = readdirSync(dir)
  .filter((file) => file.endsWith('.md'))
  .map((file) => ({ rawPath: `${dir}/${file}`, file }))
  .map(({ rawPath, file }) => {
    const fileContent = readFileSync(rawPath);
    const frontMatter = matter(fileContent);
    const { data: metadata, content } = frontMatter!;

    metadata['id'] = file.split('\.')[0];

    if (!!metadata['variants'] && !Array.isArray(metadata['variants'])) {
      metadata['variants'] = [metadata['variants']];
    }

    if (!metadata['title']) {
      metadata['title'] = content.split('\n')[0]?.replace('# ', '');
    }
    const publicPath = `${publicDir}/${file}`;
    writeFileSync(publicPath, content);

    console.log(`${file.padEnd(20, ' ')}
    \tpublicPath: ${publicPath.padEnd(50, ' ')}
    \tmetadata: ${JSON.stringify(metadata)}`);

    return {
      path: `${insideDir}/${file}`,
      ...metadata,
    };
  });

writeFileSync(`${publicDir}/manifest.json`, JSON.stringify(markdownFiles, null, 2));
