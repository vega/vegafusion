// Copyright (c) Jupyter Development Team.
// Distributed under the terms of the Modified BSD License.

// Add any needed widget imports here (or from controls)
// import {} from '@jupyter-widgets/base';

import { createTestModel } from './utils';

import { AltairFusionModel } from '..';

describe('Example', () => {
  describe('AltairFusionModel', () => {
    it('should be createable', () => {
      const model = createTestModel(AltairFusionModel);
      expect(model).toBeInstanceOf(AltairFusionModel);
      expect(model.get('value')).toEqual('Hello World');
    });

    it('should be createable with a value', () => {
      const state = { value: 'Foo Bar!' };
      const model = createTestModel(AltairFusionModel, state);
      expect(model).toBeInstanceOf(AltairFusionModel);
      expect(model.get('value')).toEqual('Foo Bar!');
    });
  });
});
