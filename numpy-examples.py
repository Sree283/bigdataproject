
## ✅ NumPy Python Demo (Example Code)

import numpy as np

def main():
    # 1) Create arrays using nump
    a = np.array([1, 2, 3, 4])
    b = np.arange(0, 10, 2)        # [0, 2, 4, 6, 8]
    c = np.linspace(0, 1, 5)       # 5 points evenly spaced between 0 and 1
    d = np.zeros((2, 3))           # 2x3 matrix of zeros
    e = np.ones((3, 2))            # 3x2 matrix of ones
    f = np.random.default_rng().random((3, 3))  # 3x3 random floats in [0, 1)

    print("a is:", a)
    print("b:", b)
    print("c:", c)
    print("d:\n", d)
    print("e:\n", e)
    print("f:\n", f)

    # 2) Basic arithmetic (elementwise)
    print("\na + b:", a + b[:4])     # broadcast b to match a length
    print("a * 2:", a * 2)
    print("a ** 2:", a**2)

    # 3) Matrix operations
    M = np.array([[1, 2], [3, 4]])
    N = np.array([[5, 6], [7, 8]])
    print("\nM:\n", M)
    print("N:\n", N)
    print("M + N:\n", M + N)
    print("M @ N (matrix multiply):\n", M @ N)
    print("M.T (transpose):\n", M.T)
    print("det(M):", np.linalg.det(M))

    # 4) Indexing / slicing
    x = np.arange(12).reshape(3, 4)
    print("\nx:\n", x)
    print("x[1, 2] ->", x[1, 2])
    print("x[0] (first row):", x[0])
    print("x[:, 1] (second column):", x[:, 1])
    print("x[1:, 2:] (submatrix):\n", x[1:, 2:])

    # 5) Universal functions (ufuncs)
    print("\nnp.sin(a):", np.sin(a))
    print("np.log(a + 1):", np.log(a + 1))
    print("np.exp(a):", np.exp(a))

    # 6) Aggregations / stats
    print("\nSum:", x.sum())
    print("Mean:", x.mean())
    print("Standard deviation:", x.std())
    print("Min / Max:", x.min(), "/", x.max())

    # 7) Boolean masking
    odds = x[x % 2 == 1]
    print("\nOdd values in x:", odds)

    # 8) Reshaping and stacking
    stacked = np.vstack([a, b[:4]])
    print("\nstacked (vstack):\n", stacked)
    reshaped = a.reshape((2, 2))
    print("a reshaped to 2x2:\n", reshaped)

if __name__ == "__main__":
    main()