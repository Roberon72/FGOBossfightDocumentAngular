import { Injectable, signal, WritableSignal } from '@angular/core';

export type UserData = {
  selectedDocument: string;
  documentStates: Record<string, DocumentState>;
};

export type DocumentState = {
  displayVariant?: boolean;
  transitionPlayed?: boolean
  readStates?: Record<string, ReadState>;
};

export type ReadState = {
  spoilerRevealed: boolean;
  arrowCompleted: boolean;
};

function isUserData(obj: any): obj is UserData {
  //FIXME: Expand the check to documentStates, as long as it has records-within-records all with state: "shown" or "hidden"
  return !!obj && Object.hasOwn(obj, "selectedDocument")
}

const DEFAULT_USER_DATA: UserData = { selectedDocument: '', documentStates: {} }



@Injectable({
  providedIn: 'root',
})
export class UserDataService {
  private _userData: WritableSignal<UserData> = signal(this.loadUserData())
  public userData = this._userData.asReadonly()

  private loadUserData(): UserData {
    const savedData = localStorage.getItem("userData")
    if (!savedData) return structuredClone(DEFAULT_USER_DATA);

    try {
      const parsed = JSON.parse(savedData);
      if (isUserData(parsed)) return parsed
    } catch(_) {}

    return structuredClone(DEFAULT_USER_DATA);
  }

  public saveUserData(userData?: UserData) {
    if (!userData) {
      localStorage.setItem("userData", JSON.stringify(DEFAULT_USER_DATA))
      return
    }

    localStorage.setItem("userData", JSON.stringify(userData))
    this._userData.set(userData)
  }
}
