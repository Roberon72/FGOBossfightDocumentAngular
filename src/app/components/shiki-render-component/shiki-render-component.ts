import { Component, computed, ElementRef, inject, input, resource } from '@angular/core';
import { codeToHtml } from 'shiki';

@Component({
  selector: 'shiki-render-component',
  imports: [],
  template: ``,
})
export class ShikiRenderComponent {
  private el = inject(ElementRef<ShikiRenderComponent>);

  code = input('code');
  lang = input('lang');

  private content = resource({
    params: () => ({ code: this.code(), lang: this.lang() }),
    loader: async ({ params: { code, lang } }) => {
      const markup = await codeToHtml(code, {
        lang,
        themes: {
          light: 'github-dark',
          dark: 'github-light',
        },
      });

      this.el.nativeElement.innerHTML = markup;
    }
  });
}
