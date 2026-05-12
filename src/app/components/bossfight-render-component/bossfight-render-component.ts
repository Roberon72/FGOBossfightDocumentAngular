import { Component, computed, effect, inject, input, linkedSignal, resource, signal } from '@angular/core';
import { BossfightRecord } from '../../services/bossfight-data-service';
import { BossfightDocumentProcessorService, } from '../../services/bossfight-document-processor-service';
import { MatDivider } from '@angular/material/list';
import { MatAccordion, MatExpansionPanel, MatExpansionPanelHeader, MatExpansionPanelTitle } from '@angular/material/expansion';
import { MatCheckbox } from '@angular/material/checkbox';
import { ReadState, UserDataService } from '../../services/user-data-service';
import deepEqualCheck from 'deep-equal-check';


@Component({
  selector: 'bossfight-render-component',
  imports: [
    MatDivider,
    MatAccordion,
    MatExpansionPanel,
    MatExpansionPanelHeader,
    MatExpansionPanelTitle,
    MatCheckbox,
  ],
  templateUrl: './bossfight-render-component.html',
  styleUrl: './bossfight-render-component.scss',
})
export class BossfightRenderComponent {
  private bossfightDocumentProcessor = inject(BossfightDocumentProcessorService);
  private userDataService = inject(UserDataService);

  private userData = this.userDataService.userData;

  bossfightDocumentRecord = input.required<BossfightRecord>({ alias: 'documentRecord' });

  protected bossfightMarkdown = resource({
    params: () => ({ documentUrl: this.bossfightDocumentRecord().path }),
    loader: ({ params: { documentUrl } }) => fetch(documentUrl).then((r) => r.text()),
    defaultValue: '# Loading...',
  });

  protected blocks = computed(() =>
    this.bossfightDocumentProcessor.parseMarkdownDocument(
      this.bossfightMarkdown.value(),
    ),
  );
  protected expandedNodes = linkedSignal({
    source: this.userData,
    computation: (source) => {
      const documentStates = source.documentStates ?? {};
      const documentReadState = documentStates[this.bossfightDocumentRecord().id] ?? {};
      const currentDocumentArrows = documentReadState.readStates ?? {};

      const result = new Set<string>();
      Object.entries(currentDocumentArrows).forEach(([arrowId, { spoilerRevealed }]) => {
        if (spoilerRevealed) {
          result.add(arrowId);
        }
      });

      return result;
    }
  });

  protected completedArrows = linkedSignal({
    source: this.userData,
    computation: (source) => {
      const documentStates = source.documentStates ?? {};
      const documentReadState = documentStates[this.bossfightDocumentRecord().id] ?? {};
      const currentDocumentArrows = documentReadState.readStates ?? {};

      const result = new Set<string>();
      Object.entries(currentDocumentArrows).forEach(([arrowId, { arrowCompleted }]) => {
        if (arrowCompleted) {
          result.add(arrowId);
        }
      });

      return result;
    },
  });

  //FIXME: Pretty bad, runs twice
  // If only I wasn't doing this in my spare time
  private updateUserData = effect(() => {
    const documentId = this.bossfightDocumentRecord().id;

    //Ugly as all hell
    const userData = structuredClone(this.userData());
    const readState: Record<string, ReadState> = {};

    const completeAndRevealed = this.expandedNodes().intersection(this.completedArrows());
    const completeOnly = this.completedArrows().difference(this.expandedNodes());
    const expandedOnly = this.expandedNodes().difference(this.completedArrows());

    completeAndRevealed.forEach((arrowId) => {
      readState[arrowId] = { arrowCompleted: true, spoilerRevealed: true };
    });

    completeOnly.forEach((arrowId) => {
      readState[arrowId] = { arrowCompleted: true, spoilerRevealed: false };
    });

    expandedOnly.forEach((arrowId) => {
      readState[arrowId] = { arrowCompleted: false, spoilerRevealed: true };
    });

    //TODO: Low-key I might import lodash just for this...
    // Alternatively, is there a deep-merge in vanilla JS?
    userData.documentStates ??= {}
    userData.documentStates[documentId] ??= {};
    userData.documentStates[documentId].readStates ??= {};

    if (deepEqualCheck(userData.documentStates[documentId].readStates, readState)) {
      return;
    }
    userData.documentStates[documentId].readStates = readState;
    this.userDataService.saveUserData(userData);
  });

  markArrowExpanded(arrowId: string, expanded: boolean) {
    this.expandedNodes.update((currentSet) => {
      const copy = new Set<string>(currentSet);
      if (expanded) {
        copy.add(arrowId);
      } else {
        copy.delete(arrowId);
      }
      return copy;
    });
  }

  markArrowCompleted(arrowId: string, checked: boolean) {
    this.completedArrows.update((currentSet) => {
      const copy = new Set<string>(currentSet);
      if (checked) {
        copy.add(arrowId);
      } else {
        copy.delete(arrowId);
      }
      return copy;
    });
  }
}
