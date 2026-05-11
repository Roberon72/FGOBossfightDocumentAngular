import { Injectable } from '@angular/core';
import { marked, Tokens, TokensList } from 'marked';

export interface ListItem {
  id: string;
  text: string;
  spoilers: string[];
}

export type BlockType = 'heading' | 'list' | 'thematic-break' | 'paragraph' | 'inline-html';

export interface Block {
  type: BlockType;
  level?: number;
  html?: string;
  plainText?: string;
  items?: ListItem[];
}

@Injectable({
  providedIn: 'root',
})
export class BossfightDocumentProcessorService {
  private readonly SPOILER_PATTERN = /\|\|(.+?)\|\|/g;

  parseMarkdownDocument(markdown: string): Block[] {
    const tokens = marked.lexer(markdown);
    const blocks: Block[] = [];
    let lastHeadingText = '';

    for (const token of tokens) {
      switch (token.type) {
        case 'heading': {
          const h = token as Tokens.Heading;
          lastHeadingText = h.text;
          const rawWithoutHash = h.raw.replace(/^#{1,6}\s+/, '');
          blocks.push({
            type: 'heading',
            level: h.depth,
            html: marked.parseInline(rawWithoutHash) as string,
            plainText: h.text
          });
          break;
        }
        case 'html': {
          blocks.push({
            type: 'inline-html',
            html: token.raw
          });
          break;
        }
        case 'list': {
          const list = token as Tokens.List;
          const items: ListItem[] = list.items.map((itemToken, index) => {
            const raw = this.getRawText(itemToken);
            return this.parseListItem(raw, lastHeadingText, index);
          });
          blocks.push({ type: 'list', items });
          break;
        }
        case 'paragraph': {
          const p = token as Tokens.Paragraph;
          blocks.push({
            type: 'paragraph',
            html: marked.parseInline(p.raw) as string
          });
          break;
        }
        case 'hr':
        case 'thematic_break': {
          blocks.push({ type: 'thematic-break' });
          break;
        }
        default:
          break;
      }
    }
    return blocks;
  }

  private getRawText(item: Tokens.ListItem): string {
    return item.tokens ? item.tokens.map(t => t.raw).join('') : item.raw || '';
  }

  private parseListItem(raw: string, heading: string, index: number): ListItem {
    const spoilers: string[] = [];
    const matches = raw.matchAll(this.SPOILER_PATTERN);
    for (const m of matches) spoilers.push(m[1].replaceAll("`", "").trim());

    let cleanText = raw.replace(this.SPOILER_PATTERN, '').trim();
    cleanText = cleanText.replace(/\s+/g, ' ').trim();

    const id = this.listItemId(heading, cleanText, index);
    return { id, text: cleanText, spoilers };
  }

  private listItemId(heading: string, text: string, index: number): string {
    return `li-${this.hash(`${heading}||${text}||${index}`)}`;
  }

  private hash(str: string): string {
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) + hash) + str.charCodeAt(i);
      hash = hash & hash;
    }
    return Math.abs(hash).toString(36).padStart(6, '0');
  }
}
