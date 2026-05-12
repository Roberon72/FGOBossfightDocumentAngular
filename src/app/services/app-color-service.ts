import { computed, DOCUMENT, effect, inject, Injectable, signal } from '@angular/core';
import { argbFromHex, DynamicScheme, Hct, hexFromArgb, Variant } from '@material/material-color-utilities';
import { Nullable } from '../app';

const DEFAULT_SCHEME_OPTIONS = {
  contrastLevel: 5,
  variant: Variant.VIBRANT,
  platform: 'phone',
  specVersion: '2025',
} as const;

@Injectable({
  providedIn: 'root',
})
export class AppColorService {
  private appDocument = inject(DOCUMENT);
  private stylesheet = new CSSStyleSheet();

  private sourceColor = signal<string | null>(null); //TODO: set back to null
  private hct = computed<Nullable<Hct>>(() => {
    const color = this.sourceColor();
    if (!color) return null;
    return Hct.fromInt(argbFromHex(color));
  });

  private updateStylesheet = effect(() => {
    if (!this.sourceColor()) {
      this.stylesheet.replaceSync('');
      return;
    }

    const lightScheme = new DynamicScheme({
      ...DEFAULT_SCHEME_OPTIONS,
      sourceColorHct: this.hct()!,
      isDark: false,
    } as any); //They don't export DynamicSchemeOptions, so this has to be any...

    const darkScheme = new DynamicScheme({
      ...DEFAULT_SCHEME_OPTIONS,
      sourceColorHct: this.hct()!,
      isDark: true,
    } as any); //They don't export DynamicSchemeOptions so this has to be any...

    console.log(lightScheme, darkScheme)
    const cssContent = this.assembleCSSContent(lightScheme, darkScheme);
    this.stylesheet.replaceSync(cssContent);
  });

  constructor() {
    document.adoptedStyleSheets.push(this.stylesheet);
  }

  //FIXME: Should be a util but it's not used anywhere else...
  private camelToKebab(value: string) {
    return value.replace(/[A-Z]+(?![a-z])|[A-Z]/g, ($, ofs) => (ofs ? '-' : '') + $.toLowerCase());
  }

  private assembleCSSContent(lightScheme: DynamicScheme, darkScheme: DynamicScheme) {
    const mainColorMap =
      Object.entries(DynamicSchemeProps).reduce((acc, [key, descriptor]) => {
        const { get: getter, set = undefined, writable = false } = descriptor
        if (!!getter) {
          acc[`--mat-sys-${this.camelToKebab(key)}`] = `light-dark(${hexFromArgb(getter.apply(lightScheme))}, ${hexFromArgb(getter.apply(darkScheme))})`;
        }
        return acc
      }, {} as Record<string, string>)

    return `:root { ${Object.entries(mainColorMap).map(([key, value]) => `${key}: ${value};`).join('\n')} }`
  }

  public updateColor(hexString: string | null) {
    this.sourceColor.set(hexString);
  }
}

const DynamicSchemeProps = Object.getOwnPropertyDescriptors(DynamicScheme.prototype);
