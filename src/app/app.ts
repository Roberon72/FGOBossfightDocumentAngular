import { Component, computed, effect, inject, linkedSignal, signal } from '@angular/core';
import { MatToolbar } from '@angular/material/toolbar';
import { MatFormField, MatOption, MatSelect, MatSelectTrigger } from '@angular/material/select';
import { UserData, UserDataService } from './services/user-data-service';
import { BossfightDataService, BossfightRecord } from './services/bossfight-data-service';
import { MatSnackBar, MatSnackBarRef } from '@angular/material/snack-bar';
import { Subscription } from 'rxjs';
import { FormsModule } from '@angular/forms';
import { JsonPipe, NgTemplateOutlet } from '@angular/common';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { BossfightRenderComponent } from './components/bossfight-render-component/bossfight-render-component';
import { AppColorService } from './services/app-color-service';

export type Nullable<T> = T | null

@Component({
  selector: 'app-root',
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
    JsonPipe,
  ],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {
  private bossfightDataService = inject(BossfightDataService);
  private userDataService = inject(UserDataService);
  private appColorService = inject(AppColorService);
  private _snackBar = inject(MatSnackBar);
  private snackBarRef = signal<MatSnackBarRef<any> | undefined>(undefined);

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
  private snackBarActionSubscription = linkedSignal<
    MatSnackBarRef<any> | undefined,
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
      if (!bossfightData?.length)
        return selectedDocument

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

  protected selectedDocumentRecord = computed(() => {
    if (!this.bossfightDataLoading() && !!this.bossfightData()) {
      return this.bossfightData()?.find(({ id }) => id == this.selectedBossfight());
    }

    return undefined;
  });
  private updateAppBackground = effect(() => {
    const baseColor = this.selectedDocumentRecord()?.baseColor ?? null;
    this.appColorService.updateColor(baseColor);
  })
}
