import { Injectable, signal, WritableSignal } from '@angular/core';
import { httpResource } from '@angular/common/http';

export interface BossfightMetadata {
  id: string;
  path: string;
  title: string;
  icon?: string | null;
  variants?: string[];
  variantTransitions?: string[];
}
export type BossfightRecord = Omit<BossfightMetadata, 'variants'> & {
  variants?: BossfightRecord[];
};
export type BossfightManifest = Array<BossfightMetadata>;
function isManifest(elem: any): elem is BossfightManifest {
  return (
    Array.isArray(elem) &&
    elem.every(({ path, title }) => !!path && (!!title || '' === title?.trim()))
  );
}

function processRawBossfight(
  bossfightMetadata: BossfightMetadata,
  _: number,
  bossfights: Array<BossfightMetadata>,
): BossfightRecord {
  const result = structuredClone(bossfightMetadata);
  const variants = result.variants
    ?.map((variantId) => {
      const variantIdx = bossfights.findIndex(({ id }) => id === variantId);
      if (variantIdx === -1) {
        console.warn(`Document refers to non-existent or already claimed variant: ${result.id} => ${variantId}`)
        return null;
      }

      const variantMetadata = bossfights.splice(variantIdx, 1)[0];
      return processRawBossfight(variantMetadata, -1, bossfights);
    })
    ?.filter((variant) => !!variant);

  return {
    ...result,
    variants,
  };
}

@Injectable({
  providedIn: 'root',
})
export class BossfightDataService {
  private readonly _bossfights = httpResource<BossfightRecord[]>(
    () => ({
      url: 'bossfights/manifest.json',
      reportProgress: true,
    }),
    {
      parse: (raw) => {
        if (!isManifest(raw)) throw Error('Manifest is not properly formatted');

        const bossfights = structuredClone(raw);
        return bossfights.map(processRawBossfight).filter(Boolean);
      },
    },
  );
  private readonly _rawArray: WritableSignal<BossfightMetadata[] | undefined> = signal(undefined);

  public readonly rawMetadata = this._rawArray.asReadonly();

  public readonly isLoading = this._bossfights.isLoading;
  public readonly error = this._bossfights.error;
  public readonly data = this._bossfights.value;

  public reload() {
    this._bossfights.reload();
  }
}
