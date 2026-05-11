import { Injectable, signal, WritableSignal } from '@angular/core';

//TODO: Move
export type UserData = {
  selectedDocument: string;
  documentStates?: Record<string, DocumentState>;
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

@Injectable({
  providedIn: 'root',
})
export class UserDataService {
  private _userData: WritableSignal<UserData | null> = signal(this.loadUserData())
  public userData = this._userData.asReadonly()

  private loadUserData(): UserData | null {
    const savedData = localStorage.getItem("userData")
    if (!savedData) return null;

    try {
      const parsed = JSON.parse(savedData);
      if (isUserData(parsed)) return parsed
    } catch(_) {}

    return null;
  }

  public saveUserData(userData?: UserData) {
    if (!userData) {
      localStorage.removeItem("userData")
      return
    }

    //TODO: check maybe?
    localStorage.setItem("userData", JSON.stringify(userData))
    this._userData.set(userData)
  }
}
