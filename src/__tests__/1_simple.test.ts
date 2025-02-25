import { MyEpicClass } from '../'

test('adds two numbers', () => {
  const c = new MyEpicClass()
  expect(c.add(2, 3)).toBe(5)
})
