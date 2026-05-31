import { Injectable, signal, WritableSignal } from '@angular/core';
import { httpResource } from '@angular/common/http';

export interface BossfightMetadata {
  id: string;
  path: string;
  title: string;
  icon?: string | null;
  baseColor?: string;
  inverted?: boolean;
  variant?: string;
  variantTransition?: string;
  isVariant?: true;
}
export type BossfightRecord = Omit<BossfightMetadata, 'variant'> & {
  variant?: BossfightRecord;
  isVariant?: true;
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
  rawBossfights: Array<BossfightMetadata>,
): BossfightRecord {
  const metadata = structuredClone(bossfightMetadata);
  const variantId = metadata.variant;
  const variant = (() => {
    if (variantId) {
      const variantMetadataIdx = rawBossfights
        .filter((e) => !!e)
        .findIndex(({ id }) => id === variantId);

      if (variantMetadataIdx === -1) {
        console.warn(
          `Document refers to non-existent or already claimed variant: ${metadata.id} => ${variantId}`,
        );
        return null;
      }
      const variant = rawBossfights[variantMetadataIdx];
      return processRawBossfight(variant, rawBossfights);
    }

    return null;
  })();

  const result: BossfightRecord = metadata as BossfightRecord;

  if (!!variant) {
    result.variant = variant;
  } else {
    delete result.variant;
  }

  return result;
}

@Injectable({
  providedIn: 'root',
})
export class BossfightDataService {
  private readonly _bossfights = httpResource<BossfightRecord[] | null>(
    () => ({
      url: 'bossfights/manifest.json?' + new Date().getTime(),
      reportProgress: true,
      headers: {
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
        Expires: 'Sat, 01 Jan 2000 00:00:00 GMT',
      },
    }),
    {
      parse: (raw) => {
        if (!isManifest(raw)) throw Error('Manifest is not properly formatted');

        const bossfights = structuredClone(raw);
        return bossfights
          .filter((e) => !!e && !e.isVariant)
          .map((bossfight) => processRawBossfight(bossfight, bossfights));
      },
      defaultValue: null,
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
