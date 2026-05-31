import {
  Component,
  computed,
  effect,
  inject,
  linkedSignal,
  signal,
  TemplateRef,
  viewChild,
} from '@angular/core';
import { MatToolbar } from '@angular/material/toolbar';
import { MatFormField, MatOption, MatSelect, MatSelectTrigger } from '@angular/material/select';
import { UserData, UserDataService } from './services/user-data-service';
import { BossfightDataService, BossfightRecord } from './services/bossfight-data-service';
import { MatSnackBar, MatSnackBarRef } from '@angular/material/snack-bar';
import { Subscription } from 'rxjs';
import { FormsModule } from '@angular/forms';
import { NgTemplateOutlet } from '@angular/common';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { BossfightRenderComponent } from './components/bossfight-render-component/bossfight-render-component';
import { AppColorService } from './services/app-color-service';
import deepEqualCheck from 'deep-equal-check';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';

export type Nullable<T> = T | null;

@Component({
  selector: 'app-root',
  host: {
    '(document:keydown)': 'listenForTransitionSignal($event)',
  },
  imports: [
    MatToolbar,
    MatSelect,
    MatOption,
    FormsModule,
    MatFormField,
    MatProgressSpinner,
    MatSelectTrigger,
    NgTemplateOutlet,
    BossfightRenderComponent,
  ],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {
  private bossfightDataService = inject(BossfightDataService);
  private userDataService = inject(UserDataService);
  private appColorService = inject(AppColorService);
  private _snackBar = inject(MatSnackBar);
  private _dialog = inject(MatDialog);
  private transitionPlayerTemplate = viewChild.required<TemplateRef<any>>('transitionPlayer');

  private dialogRef = signal<Nullable<MatDialogRef<any>>>(null);
  private snackBarRef = signal<Nullable<MatSnackBarRef<any>>>(null);

  protected userData = this.userDataService.userData;
  protected bossfightDataLoading = this.bossfightDataService.isLoading;
  protected bossfightData = this.bossfightDataService.data;
  protected bossfightDataError = this.bossfightDataService.error;

  private bossfightError = effect(() => {
    const error = this.bossfightDataError();
    if (!!error) {
      this.snackBarRef.set(this._snackBar.open(error.message, 'Reload'));
    } else {
      this._snackBar.dismiss();
    }
  });
  private dialogActionSubscription = linkedSignal({
    source: this.dialogRef,
    computation: (source, previous) => {},
  });

  private snackBarActionSubscription = linkedSignal<
    Nullable<MatSnackBarRef<any>>,
    Subscription | undefined
  >({
    source: this.snackBarRef,
    computation: (source, previous) => {
      if (!!previous) {
        previous.value?.unsubscribe();
      }

      return source?.onAction()?.subscribe(() => this.bossfightDataService.reload());
    },
  });

  protected selectedBossfight = linkedSignal<Nullable<BossfightRecord[]>, string>({
    source: this.bossfightData,
    computation: (bossfightData, previous) => {
      const { selectedDocument } = this.userData();
      if (!bossfightData?.length) return selectedDocument;

      const bossfightIds = new Set(bossfightData?.map(({ id }) => id));
      const currentSelected = previous?.value ?? selectedDocument;
      if (bossfightIds.has(currentSelected)) {
        return currentSelected;
      } else {
        return bossfightData?.at(-1)?.id ?? '';
      }
    },
  });

  private onSelectionChange = effect(() => {
    const selectedId = this.selectedBossfight();
    const currentData: UserData = this.userData() ?? { selectedDocument: '' };

    if (selectedId && currentData?.selectedDocument !== selectedId) {
      console.log('Updating user data with selected bossfight:', selectedId);
      this.userDataService.saveUserData({
        ...currentData,
        selectedDocument: selectedId,
      });
    }
  });

  protected selectedDocumentRecord = computed(
    () => {
      if (!this.bossfightDataLoading() && !!this.bossfightData()) {
        const bossfight = this.bossfightData()?.find(({ id }) => id == this.selectedBossfight());
        if (!bossfight) return undefined;

        return this.mergeVariantData(bossfight);
      }

      return undefined;
    },
    { equal: deepEqualCheck },
  );
  private updateAppBackground = effect(() => {
    const { baseColor: hexColor = null, inverted = false } = this.selectedDocumentRecord() ?? {
      inverted: false,
    };
    this.appColorService.updateTheme({ hexColor, inverted });
  });

  protected mergeVariantData(bossfightRecord: BossfightRecord): BossfightRecord {
    const { displayVariant } = this.userData().documentStates[bossfightRecord.id] ?? {};
    if (displayVariant && !!bossfightRecord.variant) {
      return { ...bossfightRecord.variant, id: bossfightRecord.id };
    }
    return bossfightRecord;
  }

  //FIXME: Horrible code
  protected listenForTransitionSignal(event: KeyboardEvent) {
    const { key, ctrlKey, shiftKey } = event;
    if (ctrlKey && shiftKey && key === 'L') {
      const userData = structuredClone(this.userData()),
        documentState = userData.documentStates[this.selectedBossfight()] ?? {},
        { displayVariant = false, transitionPlayed = false } = documentState,
        newState = structuredClone(documentState),
        documentRecord = this.selectedDocumentRecord();

      newState.displayVariant = !displayVariant;

      if (!displayVariant) {
        if (newState.displayVariant && !transitionPlayed && !!documentRecord?.variantTransition) {
          if (!!this.dialogRef()) return;

          const dialogRef = this._dialog.open(this.transitionPlayerTemplate(), {
            data: documentRecord.variantTransition,
            height: '100dvh',
            width: '100dvw',
            minHeight: '100dvh',
            minWidth: '100dvw',
            maxWidth: '100dvw',
            maxHeight: '100dvh',
            closePredicate: (result) => result === 'transitionEnd',
          });

          dialogRef.afterClosed().subscribe((result) => {
            newState.transitionPlayed = true;
            userData.documentStates[this.selectedBossfight()] = newState;
            this.userDataService.saveUserData(userData);
          });

          this.dialogRef.set(dialogRef);
        } else {
          userData.documentStates[this.selectedBossfight()] = newState;
          this.userDataService.saveUserData(userData);
        }
      } else {
        userData.documentStates[this.selectedBossfight()] = newState;
        this.userDataService.saveUserData(userData);
      }
    }
  }

  transitionEnded() {
    this.dialogRef()?.close('transitionEnd');
  }
}
