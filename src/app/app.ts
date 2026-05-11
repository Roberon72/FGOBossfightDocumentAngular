import { Component, computed, effect, inject, linkedSignal, signal } from '@angular/core';
import { MatToolbar } from '@angular/material/toolbar';
import { MatFormField, MatOption, MatSelect, MatSelectTrigger } from '@angular/material/select';
import { UserDataService } from './services/user-data-service';
import { BossfightDataService } from './services/bossfight-data-service';
import { MatSnackBar, MatSnackBarRef } from '@angular/material/snack-bar';
import { Subscription } from 'rxjs';
import { FormsModule } from '@angular/forms';
import { NgTemplateOutlet } from '@angular/common';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { BossfightRenderComponent } from './components/bossfight-render-component/bossfight-render-component';

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
    BossfightRenderComponent
  ],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {
  private bossfightDataService = inject(BossfightDataService);
  private userDataService = inject(UserDataService);
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

  protected selectedBossfight = signal<string>('');
  private initSelectedBossfight = effect(() => {
    const data = this.userData();
    if (data?.selectedDocument) {
      this.selectedBossfight.set(data.selectedDocument);
    }
    // Cleanup: only run once
    setTimeout(() => this.initSelectedBossfight.destroy());
  });

  private onSelectionChange = effect(() => {
    const selectedId = this.selectedBossfight();
    const currentData = this.userData();

    if (selectedId && currentData && currentData.selectedDocument !== selectedId) {
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
}
